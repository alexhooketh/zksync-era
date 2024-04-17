//! Consensus-related functionality.

#![allow(clippy::redundant_locals)]
#![allow(clippy::needless_pass_by_ref_mut)]

use zksync_concurrency::{ctx, error::Wrap as _, scope};
use zksync_consensus_executor as executor;
use zksync_consensus_roles::validator;
use zksync_consensus_storage::BlockStore;

pub use self::{fetcher::*, storage::Store};

pub mod config;
pub mod era;
mod fetcher;
mod storage;
#[cfg(test)]
pub(crate) mod testonly;
#[cfg(test)]
mod tests;

/// Main node consensus config.
#[derive(Debug, Clone)]
pub struct MainNodeConfig {
    pub executor: executor::Config,
    pub validator_key: validator::SecretKey,
}

impl MainNodeConfig {
    /// Task generating consensus certificates for the miniblocks generated by `StateKeeper`.
    /// Broadcasts the blocks with certificates to gossip network peers.
    pub async fn run(self, ctx: &ctx::Ctx, store: Store) -> anyhow::Result<()> {
        scope::run!(&ctx, |ctx, s| async {
            let mut block_store = store.clone().into_block_store();
            block_store
                .try_init_genesis(ctx, &self.validator_key.public())
                .await
                .wrap("block_store.try_init_genesis()")?;
            let (block_store, runner) = BlockStore::new(ctx, Box::new(block_store))
                .await
                .wrap("BlockStore::new()")?;
            s.spawn_bg(runner.run(ctx));
            let executor = executor::Executor {
                config: self.executor,
                block_store,
                validator: Some(executor::Validator {
                    key: self.validator_key,
                    replica_store: Box::new(store.clone()),
                    payload_manager: Box::new(store.clone()),
                }),
            };
            executor.run(ctx).await
        })
        .await
    }
}
