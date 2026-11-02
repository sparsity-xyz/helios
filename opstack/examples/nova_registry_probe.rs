//! Read-only end-to-end probe. Never supplies a private key or sends a transaction.
use alloy::primitives::keccak256;
use helios_opstack::{config::Network, OpStackClientBuilder};
use op_alloy_rpc_types::OpTransactionRequest;
use std::time::Duration;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let feed = std::env::args()
        .nth(1)
        .ok_or_else(|| eyre::eyre!("usage: nova_registry_probe SIGNED_FEED_URL"))?;
    let client = OpStackClientBuilder::new()
        .network(Network::BaseSepolia)
        .consensus_rpc(feed)
        .execution_rpc("https://sepolia.base.org")
        .build()?;
    client.wait_synced().await?;
    // The consensus producer sends to the block-cache consumer asynchronously.
    tokio::time::sleep(Duration::from_millis(200)).await;
    for _ in 0..3 {
        let height = client.get_block_number().await?;
        println!("verified_height={height}");
        for (address, signature, argument) in [
            (
                "0x0f68E6e699f2E972998a1EcC000c7ce103E64cc8",
                "owner()",
                None,
            ),
            (
                "0x6a28D24c9AEcdceC9B021ee6960FdDE592796af7",
                "masterSecretHash()",
                None,
            ),
            (
                "0x0f68E6e699f2E972998a1EcC000c7ce103E64cc8",
                "getActiveInstances(uint256)",
                Some(49u64),
            ),
        ] {
            let mut data = keccak256(signature.as_bytes())[..4].to_vec();
            if let Some(n) = argument {
                data.extend([0u8; 24]);
                data.extend(n.to_be_bytes());
            }
            let tx: OpTransactionRequest = serde_json::from_value(
                serde_json::json!({"to":address,"input":format!("0x{}",hex::encode(data))}),
            )?;
            let value = tokio::time::timeout(
                Duration::from_secs(20),
                client.call(&tx, alloy::eips::BlockNumberOrTag::Latest.into(), None),
            )
            .await??;
            println!("{} {} = {}", address, signature, value);
        }
        tokio::time::sleep(Duration::from_secs(4)).await;
    }
    client.shutdown().await;
    Ok(())
}
