use anyhow::Context as _;
use async_trait::async_trait;
use std::fmt::Debug;
use tokio::{runtime::Handle, sync::watch};
use zksync_dal::{Connection, ConnectionPool, Core, CoreDal};
use zksync_storage::RocksDB;
use zksync_types::L1BatchNumber;

use crate::{
    PostgresStorage, ReadStorage, RocksdbStorage, RocksdbStorageBuilder, StateKeeperColumnFamily,
};

/// Factory that can produce a [`ReadStorage`] implementation on demand.
#[async_trait]
pub trait ReadStorageFactory: Debug + Send + Sync + 'static {
    async fn access_storage(
        &self,
        stop_receiver: &watch::Receiver<bool>,
        l1_batch_number: L1BatchNumber,
    ) -> anyhow::Result<Option<PgOrRocksdbStorage<'_>>>;
}

/// A [`ReadStorage`] implementation that uses either [`PostgresStorage`] or [`RocksdbStorage`]
/// underneath.
#[derive(Debug)]
pub enum PgOrRocksdbStorage<'a> {
    Postgres(PostgresStorage<'a>),
    Rocksdb(RocksdbStorage),
}

impl<'a> PgOrRocksdbStorage<'a> {
    /// Returns a [`ReadStorage`] implementation backed by Postgres
    pub async fn access_storage_pg(
        pool: &'a ConnectionPool<Core>,
        l1_batch_number: L1BatchNumber,
    ) -> anyhow::Result<PgOrRocksdbStorage<'a>> {
        let mut connection = pool.connection().await?;
        let mut dal = connection.blocks_dal();
        let miniblock_number = match dal
            .get_miniblock_range_of_l1_batch(l1_batch_number)
            .await
            .context("Failed to load the miniblock range for the latest sealed L1 batch")?
        {
            Some((_, miniblock_number)) => miniblock_number,
            None => {
                tracing::info!("Could not find latest sealed miniblock, loading from snapshot");
                let snapshot_recovery = connection
                    .snapshot_recovery_dal()
                    .get_applied_snapshot_status()
                    .await
                    .context("Failed getting snapshot recovery info")?
                    .context("Could not find snapshot, no state available")?;
                if snapshot_recovery.l1_batch_number != l1_batch_number {
                    anyhow::bail!(
                        "Snapshot contains L1 batch #{} while #{} was expected",
                        snapshot_recovery.l1_batch_number,
                        l1_batch_number
                    );
                }
                snapshot_recovery.miniblock_number
            }
        };
        tracing::debug!(%l1_batch_number, %miniblock_number, "Using Postgres-based storage");
        Ok(
            PostgresStorage::new_async(Handle::current(), connection, miniblock_number, true)
                .await?
                .into(),
        )
    }

    /// Catches up RocksDB synchronously (i.e. assumes the gap is small) and
    /// returns a [`ReadStorage`] implementation backed by caught-up RocksDB.
    pub async fn access_storage_rocksdb(
        connection: &mut Connection<'_, Core>,
        rocksdb: RocksDB<StateKeeperColumnFamily>,
        stop_receiver: &watch::Receiver<bool>,
        l1_batch_number: L1BatchNumber,
    ) -> anyhow::Result<Option<PgOrRocksdbStorage<'a>>> {
        tracing::debug!("Catching up RocksDB synchronously");
        let rocksdb_builder = RocksdbStorageBuilder::from_rocksdb(rocksdb);
        let rocksdb = rocksdb_builder
            .synchronize(connection, stop_receiver, None)
            .await
            .context("Failed to catch up state keeper RocksDB storage to Postgres")?;
        let Some(rocksdb) = rocksdb else {
            tracing::info!("Synchronizing RocksDB interrupted");
            return Ok(None);
        };
        let rocksdb_l1_batch_number = rocksdb.l1_batch_number().await.unwrap_or_default();
        if l1_batch_number != rocksdb_l1_batch_number {
            anyhow::bail!(
                "RocksDB synchronized to L1 batch #{} while #{} was expected",
                rocksdb_l1_batch_number,
                l1_batch_number
            );
        }
        tracing::debug!(%rocksdb_l1_batch_number, "Using RocksDB-based storage");
        Ok(Some(rocksdb.into()))
    }
}

impl ReadStorage for PgOrRocksdbStorage<'_> {
    fn read_value(&mut self, key: &zksync_types::StorageKey) -> zksync_types::StorageValue {
        match self {
            Self::Postgres(postgres) => postgres.read_value(key),
            Self::Rocksdb(rocksdb) => rocksdb.read_value(key),
        }
    }

    fn is_write_initial(&mut self, key: &zksync_types::StorageKey) -> bool {
        match self {
            Self::Postgres(postgres) => postgres.is_write_initial(key),
            Self::Rocksdb(rocksdb) => rocksdb.is_write_initial(key),
        }
    }

    fn load_factory_dep(&mut self, hash: zksync_types::H256) -> Option<Vec<u8>> {
        match self {
            Self::Postgres(postgres) => postgres.load_factory_dep(hash),
            Self::Rocksdb(rocksdb) => rocksdb.load_factory_dep(hash),
        }
    }

    fn get_enumeration_index(&mut self, key: &zksync_types::StorageKey) -> Option<u64> {
        match self {
            Self::Postgres(postgres) => postgres.get_enumeration_index(key),
            Self::Rocksdb(rocksdb) => rocksdb.get_enumeration_index(key),
        }
    }
}

impl<'a> From<PostgresStorage<'a>> for PgOrRocksdbStorage<'a> {
    fn from(value: PostgresStorage<'a>) -> Self {
        Self::Postgres(value)
    }
}

impl<'a> From<RocksdbStorage> for PgOrRocksdbStorage<'a> {
    fn from(value: RocksdbStorage) -> Self {
        Self::Rocksdb(value)
    }
}
