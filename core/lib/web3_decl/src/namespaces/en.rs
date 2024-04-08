use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use zksync_config::{configs::EcosystemContracts, GenesisConfig};
use zksync_types::{api::en, tokens::TokenInfo, Address, MiniblockNumber};

#[cfg_attr(
    all(feature = "client", feature = "server"),
    rpc(server, client, namespace = "en")
)]
#[cfg_attr(
    all(feature = "client", not(feature = "server")),
    rpc(client, namespace = "en")
)]
#[cfg_attr(
    all(not(feature = "client"), feature = "server"),
    rpc(server, namespace = "en")
)]
pub trait EnNamespace {
    #[method(name = "syncL2Block")]
    async fn sync_l2_block(
        &self,
        block_number: MiniblockNumber,
        include_transactions: bool,
    ) -> RpcResult<Option<en::SyncBlock>>;

    #[method(name = "consensusGenesis")]
    async fn consensus_genesis(&self) -> RpcResult<Option<en::ConsensusGenesis>>;

    /// Lists all tokens created at or before the specified `block_number`.
    ///
    /// This method is used by EN after snapshot recovery in order to recover token records.
    #[method(name = "syncTokens")]
    async fn sync_tokens(&self, block_number: Option<MiniblockNumber>)
        -> RpcResult<Vec<TokenInfo>>;

    /// Get genesis configuration
    #[method(name = "genesisConfig")]
    async fn genesis_config(&self) -> RpcResult<GenesisConfig>;

    /// Get tokens that are white-listed and it can be used by paymasters.
    #[method(name = "whitelistedTokensForAA")]
    async fn whitelisted_tokens_for_aa(&self) -> RpcResult<Vec<Address>>;

    #[method(name = "getEcosystemContracts")]
    async fn get_ecosystem_contracts(&self) -> RpcResult<EcosystemContracts>;
}
