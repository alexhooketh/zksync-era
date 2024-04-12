use std::{
    fmt::{Debug, Formatter},
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use zksync_dal::{ConnectionPool, Core, CoreDal};
use zksync_types::L1BatchNumber;

use crate::db_pruner::PruneCondition;

pub struct L1BatchOlderThanPruneCondition {
    pub minimal_age: Duration,
    pub conn: ConnectionPool<Core>,
}

impl Debug for L1BatchOlderThanPruneCondition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "l1 Batch is older than {:?}", self.minimal_age)
    }
}

#[async_trait]
impl PruneCondition for L1BatchOlderThanPruneCondition {
    async fn is_batch_prunable(&self, l1_batch_number: L1BatchNumber) -> anyhow::Result<bool> {
        let mut storage = self.conn.connection().await?;
        let l1_batch_header = storage
            .blocks_dal()
            .get_l1_batch_header(l1_batch_number)
            .await?;
        let is_old_enough = l1_batch_header.is_some()
            && (Utc::now().timestamp() as u64 - l1_batch_header.unwrap().timestamp
                > self.minimal_age.as_secs());
        Ok(is_old_enough)
    }
}

pub struct NextL1BatchWasExecutedCondition {
    pub conn: ConnectionPool<Core>,
}

impl Debug for NextL1BatchWasExecutedCondition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "next l1 batch was executed")
    }
}

#[async_trait]
impl PruneCondition for NextL1BatchWasExecutedCondition {
    async fn is_batch_prunable(&self, l1_batch_number: L1BatchNumber) -> anyhow::Result<bool> {
        let mut storage = self.conn.connection().await?;
        let next_l1_batch_number = L1BatchNumber(l1_batch_number.0 + 1);
        let last_executed_batch = storage
            .blocks_dal()
            .get_number_of_last_l1_batch_executed_on_eth()
            .await?;
        let was_next_batch_executed =
            last_executed_batch.is_some() && last_executed_batch.unwrap() >= next_l1_batch_number;
        Ok(was_next_batch_executed)
    }
}

pub struct NextL1BatchHasMetadataCondition {
    pub conn: ConnectionPool<Core>,
}

impl Debug for NextL1BatchHasMetadataCondition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "next l1 batch has metadata")
    }
}

#[async_trait]
impl PruneCondition for NextL1BatchHasMetadataCondition {
    async fn is_batch_prunable(&self, l1_batch_number: L1BatchNumber) -> anyhow::Result<bool> {
        let mut storage = self.conn.connection().await?;
        let next_l1_batch_number = L1BatchNumber(l1_batch_number.0 + 1);
        let l1_batch_metadata = storage
            .blocks_dal()
            .get_l1_batch_metadata(next_l1_batch_number)
            .await?;
        Ok(l1_batch_metadata.is_some())
    }
}

pub struct L1BatchExistsCondition {
    pub conn: ConnectionPool<Core>,
}

impl Debug for L1BatchExistsCondition {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "l1 batch exists")
    }
}

#[async_trait]
impl PruneCondition for L1BatchExistsCondition {
    async fn is_batch_prunable(&self, l1_batch_number: L1BatchNumber) -> anyhow::Result<bool> {
        let mut storage = self.conn.connection().await?;
        let l1_batch_header = storage
            .blocks_dal()
            .get_l1_batch_header(l1_batch_number)
            .await?;
        Ok(l1_batch_header.is_some())
    }
}

pub struct ConsistencyCheckerProcessedBatch {
    pub conn: ConnectionPool<Core>,
}

impl Debug for ConsistencyCheckerProcessedBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "l1 batch was processed by consistency checker")
    }
}

#[async_trait]
impl PruneCondition for ConsistencyCheckerProcessedBatch {
    async fn is_batch_prunable(&self, l1_batch_number: L1BatchNumber) -> anyhow::Result<bool> {
        let mut storage = self.conn.connection().await?;
        let last_processed_l1_batch = storage
            .blocks_dal()
            .get_consistency_checker_last_processed_l1_batch()
            .await?;
        Ok(l1_batch_number <= last_processed_l1_batch)
    }
}
