use std::{ops::Deref, sync::Arc, time::Instant};

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use multivm::interface::{L2BlockEnv, VmInterface};
use tokio::{runtime::Handle, task::JoinHandle};
use tracing::{debug, error, info, trace, warn};
use vm_utils::{create_vm, execute_tx};
use zksync_crypto::hasher::blake2::Blake2Hasher;
use zksync_dal::{tee_verifier_input_producer_dal::JOB_MAX_ATTEMPT, ConnectionPool, Core, CoreDal};
use zksync_merkle_tree::{
    BlockOutputWithProofs, TreeInstruction, TreeLogEntry, TreeLogEntryWithProof,
};
use zksync_object_store::{ObjectStore, ObjectStoreFactory};
use zksync_prover_interface::inputs::PrepareBasicCircuitsJob;
use zksync_queued_job_processor::JobProcessor;
use zksync_types::{
    block::MiniblockExecutionData, ethabi::ethereum_types::BigEndianHash,
    tee_verifier_input::TeeVerifierInput, AccountTreeId, L1BatchNumber, L2ChainId, StorageKey,
    H256,
};

use self::metrics::METRICS;

mod metrics;
/// Component that extracts all data (from DB) necessary to run a Basic Witness Generator.
/// Does this by rerunning an entire L1Batch and extracting information from both the VM run and DB.
/// This component will upload Witness Inputs to the object store.
/// This allows Witness Generator workflow (that needs only Basic Witness Generator Inputs)
/// to be run only using the object store information, having no other external dependency.
#[derive(Debug)]
pub struct TeeVerifierInputProducer {
    connection_pool: ConnectionPool<Core>,
    l2_chain_id: L2ChainId,
    object_store: Arc<dyn ObjectStore>,
}

#[derive(Debug)]
pub struct TeeBlockOutputWithProofs {
    pub inner: BlockOutputWithProofs,
}

impl Deref for TeeBlockOutputWithProofs {
    type Target = BlockOutputWithProofs;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<PrepareBasicCircuitsJob> for TeeBlockOutputWithProofs {
    fn from(value: PrepareBasicCircuitsJob) -> Self {
        let merkle_paths = value.into_merkle_paths();
        let mut logs: Vec<TreeLogEntryWithProof> = Vec::with_capacity(merkle_paths.len());
        for path in merkle_paths {
            let merkle_path = path.merkle_paths.iter().map(|x| x.into()).collect();
            let base: TreeLogEntry =
                match (path.is_write, path.first_write, path.leaf_enumeration_index) {
                    (false, _, 0) => TreeLogEntry::ReadMissingKey,
                    (false, _, _) => TreeLogEntry::Read {
                        leaf_index: path.leaf_enumeration_index,
                        value: path.value_read.into(),
                    },
                    (true, true, _) => TreeLogEntry::Inserted,
                    (true, false, _) => TreeLogEntry::Updated {
                        leaf_index: path.leaf_enumeration_index,
                        previous_value: path.value_read.into(),
                    },
                };
            logs.push(TreeLogEntryWithProof {
                base,
                merkle_path,
                root_hash: path.root_hash.into(),
            });
        }
        TeeBlockOutputWithProofs {
            inner: BlockOutputWithProofs {
                logs,
                leaf_count: 0,
            },
        }
    }
}

impl TeeVerifierInputProducer {
    pub async fn new(
        connection_pool: ConnectionPool<Core>,
        store_factory: &ObjectStoreFactory,
        l2_chain_id: L2ChainId,
    ) -> anyhow::Result<Self> {
        Ok(TeeVerifierInputProducer {
            connection_pool,
            object_store: store_factory.create_store().await,
            l2_chain_id,
        })
    }

    fn process_job_impl(
        rt_handle: Handle,
        l1_batch_number: L1BatchNumber,
        started_at: Instant,
        connection_pool: ConnectionPool<Core>,
        object_store: Arc<dyn ObjectStore>,
        l2_chain_id: L2ChainId,
    ) -> anyhow::Result<TeeVerifierInput> {
        let job: PrepareBasicCircuitsJob = rt_handle
            .block_on(object_store.get(l1_batch_number))
            .context("failed to get PrepareBasicCircuitsJob from object store")?;

        let mut idx = job.next_enumeration_index();
        let bowp: TeeBlockOutputWithProofs = job.into();

        let mut connection = rt_handle
            .block_on(connection_pool.connection())
            .context("failed to get connection for TeeVerifierInputProducer")?;

        let old_root_hash = rt_handle
            .block_on(
                connection
                    .blocks_dal()
                    .get_l1_batch_state_root(l1_batch_number - 1),
            )?
            .ok_or(anyhow!("Failed to get old root hash"))?;

        let new_root_hash = rt_handle
            .block_on(
                connection
                    .blocks_dal()
                    .get_l1_batch_state_root(l1_batch_number),
            )?
            .ok_or(anyhow!("Failed to get new root hash"))?;
        //tracing::info!("{:?}", old_root_hash);

        let miniblocks_execution_data = rt_handle.block_on(
            connection
                .transactions_dal()
                .get_miniblocks_to_execute_for_l1_batch(l1_batch_number),
        )?;

        let fictive_miniblock_number = miniblocks_execution_data.last().unwrap().number + 1;
        let fictive_miniblock_data = rt_handle
            .block_on(
                connection
                    .sync_dal()
                    .sync_block(fictive_miniblock_number, false),
            )
            .unwrap()
            .unwrap();
        let last_non_fictive_miniblock_data = rt_handle
            .block_on(
                connection
                    .sync_dal()
                    .sync_block(fictive_miniblock_number - 1, false),
            )
            .unwrap()
            .unwrap();
        let fictive_miniblock_data = MiniblockExecutionData {
            number: fictive_miniblock_data.number,
            timestamp: fictive_miniblock_data.timestamp,
            prev_block_hash: last_non_fictive_miniblock_data.hash.unwrap_or_default(),
            virtual_blocks: fictive_miniblock_data.virtual_blocks.unwrap_or(0),
            txs: Vec::new(),
        };

        let (mut vm, _storage_view) =
            create_vm(rt_handle.clone(), l1_batch_number, connection, l2_chain_id)
                .context("failed to create vm for TeeVerifierInputProducer")?;

        info!("Started execution of l1_batch: {l1_batch_number:?}");

        let next_miniblocks_data = miniblocks_execution_data
            .iter()
            .skip(1)
            .map(Some)
            .chain([Some(&fictive_miniblock_data)]);
        let miniblocks_data = miniblocks_execution_data.iter().zip(next_miniblocks_data);

        for (miniblock_data, next_miniblock_data) in miniblocks_data {
            trace!(
                "Started execution of miniblock: {:?}, executing {:?} transactions",
                miniblock_data.number,
                miniblock_data.txs.len(),
            );
            for tx in &miniblock_data.txs {
                trace!("Started execution of tx: {tx:?}");
                execute_tx(tx, &mut vm)
                    .context("failed to execute transaction in TeeVerifierInputProducer")?;
                trace!("Finished execution of tx: {tx:?}");
            }
            if let Some(next_miniblock_data) = next_miniblock_data {
                vm.start_new_l2_block(L2BlockEnv::from_miniblock_data(next_miniblock_data));
            }

            trace!(
                "Finished execution of miniblock: {:?}",
                miniblock_data.number
            );
        }

        let vm_out = vm.finish_batch();

        let instructions: Vec<TreeInstruction> = vm_out
            .final_execution_state
            .deduplicated_storage_log_queries
            .into_iter()
            .zip(bowp.logs.iter())
            .map(|(log_query, tree_log_entry)| {
                let key = StorageKey::new(
                    AccountTreeId::new(log_query.address),
                    H256(log_query.key.into()),
                )
                .hashed_key_u256();

                Ok(match (log_query.rw_flag, tree_log_entry.base) {
                    (true, TreeLogEntry::Updated { leaf_index, .. }) => TreeInstruction::write(
                        key,
                        leaf_index,
                        H256(log_query.written_value.into()),
                    ),
                    (true, TreeLogEntry::Inserted) => {
                        let leaf_index = idx;
                        idx += 1;
                        TreeInstruction::write(
                            key,
                            leaf_index,
                            H256(log_query.written_value.into()),
                        )
                    }
                    (false, TreeLogEntry::Read { value, .. }) => {
                        if log_query.read_value != value.into_uint() {
                            error!(
                                "🛑 🛑 Failed to map LogQuery to TreeInstruction: {:#?} != {:#?}",
                                log_query.read_value, value
                            );
                            bail!(
                                "Failed to map LogQuery to TreeInstruction: {:#?} != {:#?}",
                                log_query.read_value,
                                value
                            );
                        }
                        TreeInstruction::Read(key)
                    }
                    (false, TreeLogEntry::ReadMissingKey { .. }) => TreeInstruction::Read(key),
                    _ => {
                        error!("🛑 🛑 Failed to map LogQuery to TreeInstruction");
                        bail!("Failed to map LogQuery to TreeInstruction");
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        // `verify_proofs` will panic!() if something does not add up.
        if !bowp.verify_proofs(&Blake2Hasher, old_root_hash, &instructions) {
            error!("🛑 🛑 Failed to verify_proofs {l1_batch_number} correctly - oh no!");
            bail!("Failed to verify_proofs {l1_batch_number} correctly - oh no!");
        }

        if bowp.root_hash() != Some(new_root_hash) {
            error!(
                "🛑 🛑 Failed to verify {l1_batch_number} correctly - oh no! {:#?} != {:#?}",
                bowp.root_hash(),
                new_root_hash
            );
            bail!(
                "Failed to verify {l1_batch_number} correctly - oh no! {:#?} != {:#?}",
                bowp.root_hash(),
                new_root_hash
            );
        }

        info!("🚀 Looks like we verified {l1_batch_number} correctly - whoop, whoop! 🚀");

        info!("Finished execution of l1_batch: {l1_batch_number:?}");

        METRICS.process_batch_time.observe(started_at.elapsed());
        debug!(
            "TeeVerifierInputProducer took {:?} for L1BatchNumber {}",
            started_at.elapsed(),
            l1_batch_number.0
        );

        let tee_verifier_input = TeeVerifierInput {};

        Ok(tee_verifier_input)
    }
}

#[async_trait]
impl JobProcessor for TeeVerifierInputProducer {
    type Job = L1BatchNumber;
    type JobId = L1BatchNumber;
    type JobArtifacts = TeeVerifierInput;
    const SERVICE_NAME: &'static str = "tee_verifier_input_producer";

    async fn get_next_job(&self) -> anyhow::Result<Option<(Self::JobId, Self::Job)>> {
        let mut connection = self.connection_pool.connection().await?;
        let l1_batch_to_process = connection
            .tee_verifier_input_producer_dal()
            .get_next_tee_verifier_input_producer_job()
            .await
            .context("failed to get next basic witness input producer job")?;
        Ok(l1_batch_to_process.map(|number| (number, number)))
    }

    async fn save_failure(&self, job_id: Self::JobId, started_at: Instant, error: String) {
        let attempts = self
            .connection_pool
            .connection()
            .await
            .unwrap()
            .tee_verifier_input_producer_dal()
            .mark_job_as_failed(job_id, started_at, error)
            .await
            .expect("errored whilst marking job as failed");
        if let Some(tries) = attempts {
            warn!("Failed to process job: {job_id:?}, after {tries} tries.");
        } else {
            warn!("L1 Batch {job_id:?} was processed successfully by another worker.");
        }
    }

    async fn process_job(
        &self,
        _job_id: &Self::JobId,
        job: Self::Job,
        started_at: Instant,
    ) -> JoinHandle<anyhow::Result<Self::JobArtifacts>> {
        let l2_chain_id = self.l2_chain_id;
        let connection_pool = self.connection_pool.clone();
        let object_store = self.object_store.clone();
        tokio::task::spawn_blocking(move || {
            let rt_handle = Handle::current();
            Self::process_job_impl(
                rt_handle,
                job,
                started_at,
                connection_pool.clone(),
                object_store,
                l2_chain_id,
            )
        })
    }

    async fn save_result(
        &self,
        job_id: Self::JobId,
        started_at: Instant,
        artifacts: Self::JobArtifacts,
    ) -> anyhow::Result<()> {
        let upload_started_at = Instant::now();
        let object_path = self
            .object_store
            .put(job_id, &artifacts)
            .await
            .context("failed to upload artifacts for TeeVerifierInputProducer")?;
        METRICS
            .upload_input_time
            .observe(upload_started_at.elapsed());
        let mut connection = self
            .connection_pool
            .connection()
            .await
            .context("failed to acquire DB connection for TeeVerifierInputProducer")?;
        let mut transaction = connection
            .start_transaction()
            .await
            .context("failed to acquire DB transaction for TeeVerifierInputProducer")?;
        transaction
            .tee_verifier_input_producer_dal()
            .mark_job_as_successful(job_id, started_at, &object_path)
            .await
            .context("failed to mark job as successful for TeeVerifierInputProducer")?;
        transaction
            .commit()
            .await
            .context("failed to commit DB transaction for TeeVerifierInputProducer")?;
        METRICS.block_number_processed.set(job_id.0 as i64);
        Ok(())
    }

    fn max_attempts(&self) -> u32 {
        JOB_MAX_ATTEMPT as u32
    }

    async fn get_job_attempts(&self, job_id: &L1BatchNumber) -> anyhow::Result<u32> {
        let mut connection = self
            .connection_pool
            .connection()
            .await
            .context("failed to acquire DB connection for TeeVerifierInputProducer")?;
        connection
            .tee_verifier_input_producer_dal()
            .get_tee_verifier_input_producer_job_attempts(*job_id)
            .await
            .map(|attempts| attempts.unwrap_or(0))
            .context("failed to get job attempts for TeeVerifierInputProducer")
    }
}
