use serde::{Deserialize, Serialize};

/// Storage data used as input for the TEE verifier.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TeeVerifierInput {
    // Empty struct for now
}
