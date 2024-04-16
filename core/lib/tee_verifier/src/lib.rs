//! Types for the tee_verifier

// Can't be put in `zksync_types`, because it needs `BlockOutputWithProofs`,
// which would require
// adding a circular dependency.

use multivm::interface::{L1BatchEnv, SystemEnv};
use serde::{Deserialize, Serialize};
use zksync_basic_types::{L1BatchNumber, L2ChainId, H256};
use zksync_merkle_tree::BlockOutputWithProofs;
use zksync_object_store::{serialize_using_bincode, Bucket, StoredObject};
use zksync_types::{block::MiniblockExecutionData, StorageKey, StorageValue};

/// Storage data used as input for the TEE verifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeeVerifierInput {
    pub l2_chain_id: L2ChainId,
    pub l1_batch_number: L1BatchNumber,
    pub enumeration_index: u64,
    pub block_output_with_proofs: BlockOutputWithProofs,
    pub old_root_hash: H256,
    pub new_root_hash: H256,
    pub miniblocks_execution_data: Vec<MiniblockExecutionData>,
    pub fictive_miniblock_data: MiniblockExecutionData,
    pub l1_batch_env: L1BatchEnv,
    pub system_env: SystemEnv,
    pub initial_read_values: Vec<(StorageKey, u64, StorageValue)>,
    pub used_contracts: Vec<Option<(H256, Vec<u8>)>>,
}

impl StoredObject for TeeVerifierInput {
    const BUCKET: Bucket = Bucket::TeeVerifierInput;
    type Key<'a> = L1BatchNumber;

    fn encode_key(key: Self::Key<'_>) -> String {
        format!("tee_verifier_input_for_l1_batch_{key}.bin")
    }

    serialize_using_bincode!();
}
