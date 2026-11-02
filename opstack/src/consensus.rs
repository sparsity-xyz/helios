use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use url::Url;

use alloy::consensus::proofs::{calculate_transaction_root, calculate_withdrawals_root};
use alloy::consensus::transaction::SignerRecoverable;
use alloy::consensus::{Header as ConsensusHeader, Transaction as TxTrait};
use alloy::eips::eip4895::{Withdrawal, Withdrawals};
use alloy::primitives::{b256, fixed_bytes, Address, Bloom, BloomInput, B256, U256};
use alloy::rlp::Decodable;
use alloy::rpc::types::{
    Block, EIP1186AccountProofResponse, Header, Transaction as EthTransaction,
};
use eyre::{eyre, OptionExt, Result};
use op_alloy_consensus::OpTxEnvelope;
use op_alloy_network::primitives::BlockTransactions;
use op_alloy_rpc_types::Transaction;
use tokio::sync::mpsc::Sender;
use tokio::sync::{
    mpsc::{channel, Receiver},
    watch,
};
use tracing::{debug, error, warn};

use helios_consensus_core::consensus_spec::MainnetConsensusSpec;
use helios_core::consensus::Consensus;
use helios_core::execution::proof::{verify_account_proof, verify_mpt_proof};
use helios_core::time::{interval, SystemTime, UNIX_EPOCH};
use helios_ethereum::consensus::ConsensusClient as EthConsensusClient;

use helios_ethereum::database::ConfigDB;
use helios_ethereum::rpc::http_rpc::HttpRpc;

use crate::{config::Config, types::ExecutionPayload, SequencerCommitment};

// Storage slot containing the unsafe signer address in all superchain system config contracts
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SYNC_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_HEAD_AGE: u64 = 60;

const UNSAFE_SIGNER_SLOT: &str =
    "0x65a7ed542fb37fe237fdfbdd70b31598523fe5b32879e307bae27a0bd9581c08";

pub struct ConsensusClient {
    block_recv: Option<Receiver<Block<Transaction>>>,
    finalized_block_recv: Option<watch::Receiver<Option<Block<Transaction>>>>,
    chain_id: u64,
    accepted_timestamp: watch::Receiver<Option<u64>>,
    shutdown: watch::Sender<bool>,
}

impl ConsensusClient {
    pub fn new(config: &Config) -> Self {
        let (block_send, block_recv) = channel(256);
        let (finalized_block_send, finalized_block_recv) = watch::channel(None);

        let (accepted_send, accepted_timestamp) = watch::channel(None);
        let (shutdown, mut shutdown_recv) = watch::channel(false);

        let mut inner = Inner {
            server_url: config.consensus_rpc.clone(),
            unsafe_signer: Arc::new(Mutex::new(config.chain.unsafe_signer)),
            chain_id: config.chain.chain_id,
            latest_block: None,
            accepted_send,
            http: reqwest::Client::new(),
            block_send,
            finalized_block_send,
        };

        if config.verify_unsafe_signer {
            verify_unsafe_signer(config.clone(), inner.unsafe_signer.clone());
        }

        #[cfg(not(target_arch = "wasm32"))]
        let run = tokio::spawn;

        #[cfg(target_arch = "wasm32")]
        let run = wasm_bindgen_futures::spawn_local;

        run(async move {
            let mut interval = interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = shutdown_recv.changed() => break,
                    result = inner.advance() => {
                        if let Err(e) = result {
                            error!(target: "helios::opstack", "failed to advance: {}", e);
                        }
                    }
                }
                tokio::select! {
                    _ = shutdown_recv.changed() => break,
                    _ = interval.tick() => {}
                }
            }
        });

        Self {
            block_recv: Some(block_recv),
            finalized_block_recv: Some(finalized_block_recv),
            chain_id: config.chain.chain_id,
            accepted_timestamp,
            shutdown,
        }
    }
}

#[async_trait::async_trait]
impl Consensus<Block<Transaction>> for ConsensusClient {
    fn chain_id(&self) -> u64 {
        self.chain_id
    }

    fn shutdown(&self) -> eyre::Result<()> {
        self.shutdown.send_replace(true);
        Ok(())
    }

    fn block_recv(&mut self) -> Option<Receiver<Block<Transaction>>> {
        self.block_recv.take()
    }

    fn finalized_block_recv(&mut self) -> Option<watch::Receiver<Option<Block<Transaction>>>> {
        self.finalized_block_recv.take()
    }

    fn checkpoint_recv(&self) -> Option<watch::Receiver<Option<B256>>> {
        None
    }

    fn expected_highest_block(&self) -> u64 {
        u64::MAX
    }

    async fn wait_synced(&self) -> eyre::Result<()> {
        wait_for_fresh_head(
            self.accepted_timestamp.clone(),
            self.shutdown.subscribe(),
            SYNC_TIMEOUT,
        )
        .await
    }
}

#[allow(dead_code)]
struct Inner {
    server_url: Url,
    unsafe_signer: Arc<Mutex<Address>>,
    chain_id: u64,
    latest_block: Option<u64>,
    accepted_send: watch::Sender<Option<u64>>,
    http: reqwest::Client,
    block_send: Sender<Block<Transaction>>,
    finalized_block_send: watch::Sender<Option<Block<Transaction>>>,
}

impl Inner {
    pub async fn advance(&mut self) -> Result<()> {
        let url = self
            .server_url
            .join("latest")
            .map_err(|e| eyre!("Failed to construct latest URL: {}", e))?;
        let commitment = fetch_commitment(&self.http, url, REQUEST_TIMEOUT).await?;
        self.accept(commitment).await
    }

    async fn accept(&mut self, commitment: SequencerCommitment) -> Result<()> {
        let curr_signer = *self
            .unsafe_signer
            .lock()
            .map_err(|_| eyre!("failed to lock signer"))?;
        commitment.verify(curr_signer, self.chain_id)?;
        let payload = ExecutionPayload::try_from(&commitment)?;
        check_fresh_timestamp(payload.timestamp)?;
        if self
            .latest_block
            .map(|n| payload.block_number <= n)
            .unwrap_or(false)
        {
            return Ok(());
        }
        let timestamp = payload.timestamp;
        let number = payload.block_number;
        let block = payload_to_block(payload)?;
        self.block_send
            .send(block)
            .await
            .map_err(|_| eyre!("block consumer stopped"))?;
        self.latest_block = Some(number);
        self.accepted_send.send_replace(Some(timestamp));
        debug!(target: "helios::opstack", "unsafe head updated: block={}", number);
        Ok(())
    }
}

fn check_fresh_timestamp(timestamp: u64) -> Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    if now.saturating_sub(timestamp) > MAX_HEAD_AGE || timestamp > now.saturating_add(5) {
        return Err(eyre!(
            "sequencer head timestamp is not current: {} (now {})",
            timestamp,
            now
        ));
    }
    Ok(())
}

async fn fetch_commitment(
    http: &reqwest::Client,
    url: Url,
    timeout: Duration,
) -> Result<SequencerCommitment> {
    // Use the shared native/WASM timer abstraction; cover both headers and body.
    let request = async {
        Ok(http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    };
    let mut deadline = interval(timeout);
    deadline.tick().await;
    tokio::select! {
        result = request => result,
        _ = deadline.tick() => Err(eyre!("signed-block request timed out after {:?}", timeout)),
    }
}

async fn wait_for_fresh_head(
    mut accepted: watch::Receiver<Option<u64>>,
    mut shutdown: watch::Receiver<bool>,
    timeout: Duration,
) -> Result<()> {
    let mut deadline = interval(timeout);
    deadline.tick().await;
    loop {
        if *shutdown.borrow() {
            return Err(eyre!("consensus stopped"));
        }
        if accepted
            .borrow()
            .is_some_and(|ts| check_fresh_timestamp(ts).is_ok())
        {
            return Ok(());
        }
        tokio::select! {
            result = accepted.changed() => { result.map_err(|_| eyre!("consensus task stopped"))?; },
            _ = shutdown.changed() => return Err(eyre!("consensus stopped")),
            _ = deadline.tick() => return Err(eyre!("no fresh verified sequencer head within {:?}", timeout)),
        }
    }
}

fn verify_unsafe_signer(config: Config, signer: Arc<Mutex<Address>>) {
    #[cfg(not(target_arch = "wasm32"))]
    let run = tokio::spawn;

    #[cfg(target_arch = "wasm32")]
    let run = wasm_bindgen_futures::spawn_local;

    run(async move {
        let fut = async move {
            let mut eth_config = config.chain.eth_network.to_base_config();
            eth_config.load_external_fallback = config.load_external_fallback.unwrap_or(false);

            if let Some(checkpoint) = config.checkpoint {
                eth_config.default_checkpoint = checkpoint;
            }

            let consensus_rpc = eth_config
                .consensus_rpc
                .as_ref()
                .ok_or_else(|| eyre!("missing consensus rpc"))?
                .clone();

            let mut eth_consensus =
                EthConsensusClient::<MainnetConsensusSpec, HttpRpc, ConfigDB>::new(
                    &consensus_rpc,
                    Arc::new(eth_config.into()),
                )?;

            let block = eth_consensus
                .block_recv()
                .unwrap()
                .recv()
                .await
                .ok_or_eyre("failed to receive block")?;

            // Query proof from op consensus server
            let url = config
                .consensus_rpc
                .join(&format!("unsafe_signer_proof/{}", block.header.hash))
                .map_err(|e| eyre!("Failed to construct proof URL: {}", e))?;
            let proof = reqwest::get(url)
                .await?
                .json::<EIP1186AccountProofResponse>()
                .await?;

            // Verify unsafe signer
            // with account proof
            if verify_account_proof(&proof, block.header.state_root).is_err() {
                warn!(target: "helios::opstack", "account proof invalid");
                return Err(eyre!("account proof invalid"));
            }

            // with storage proof
            let storage_proof = proof.storage_proof[0].clone();
            let key = storage_proof.key.as_b256();
            if key != B256::from_str(UNSAFE_SIGNER_SLOT)? {
                warn!(target: "helios::opstack", "account proof invalid");
                return Err(eyre!("account proof invalid"));
            }

            if verify_mpt_proof(
                proof.storage_hash,
                key,
                storage_proof.value,
                &storage_proof.proof,
            )
            .is_err()
            {
                warn!(target: "helios::opstack", "storage proof invalid");
                return Err(eyre!("storage proof invalid"));
            }

            // Replace unsafe signer if different
            let verified_signer =
                Address::from_slice(&storage_proof.value.to_be_bytes::<32>()[12..32]);
            {
                let mut curr_signer = signer.lock().map_err(|_| eyre!("failed to lock signer"))?;
                if verified_signer != *curr_signer {
                    debug!(target: "helios::opstack", "unsafe signer updated: {}", verified_signer);
                    *curr_signer = verified_signer;
                }
            }

            // Shutdown eth consensus client
            eth_consensus.shutdown()?;

            Ok(())
        };

        _ = fut.await;
    });
}

fn payload_to_block(value: ExecutionPayload) -> Result<Block<Transaction>> {
    let empty_nonce = fixed_bytes!("0000000000000000");
    let empty_uncle_hash =
        b256!("1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347");

    let txs = value
        .transactions
        .iter()
        .enumerate()
        .map(|(i, tx_bytes)| {
            let tx_bytes = tx_bytes.to_vec();
            let mut tx_bytes_slice = tx_bytes.as_slice();
            let tx_envelope = OpTxEnvelope::decode(&mut tx_bytes_slice)
                .map_err(|e| eyre!("invalid sequencer transaction: {}", e))?;
            let base_fee = tx_envelope.effective_gas_price(Some(value.base_fee_per_gas.to()));
            let recovered = tx_envelope.try_into_recovered()?;

            let inner_tx = EthTransaction {
                inner: recovered,
                block_hash: Some(value.block_hash),
                block_number: Some(value.block_number),
                transaction_index: Some(i as u64),
                effective_gas_price: Some(base_fee),
            };

            Ok(match inner_tx.inner.inner() {
                OpTxEnvelope::Legacy(_)
                | OpTxEnvelope::Eip2930(_)
                | OpTxEnvelope::Eip1559(_)
                | OpTxEnvelope::Eip7702(_) => Transaction {
                    inner: inner_tx,
                    deposit_nonce: None,
                    deposit_receipt_version: None,
                },
                OpTxEnvelope::Deposit(inner) => {
                    let deposit_nonce = Some(inner.inner().nonce());

                    Transaction {
                        inner: inner_tx,
                        deposit_nonce,
                        deposit_receipt_version: None,
                    }
                }
            })
        })
        .collect::<Result<Vec<Transaction>>>()?;
    let tx_envelopes = txs
        .iter()
        .map(|tx| tx.inner.inner.clone())
        .collect::<Vec<_>>();
    let txs_root = calculate_transaction_root(&tx_envelopes);

    let withdrawals: Vec<Withdrawal> = value.withdrawals.into_iter().map(|w| w.into()).collect();
    let withdrawals_root = calculate_withdrawals_root(&withdrawals);

    let logs_bloom: Bloom = Bloom::from(BloomInput::Raw(&value.logs_bloom));

    let consensus_header = ConsensusHeader {
        parent_hash: value.parent_hash,
        ommers_hash: empty_uncle_hash,
        beneficiary: Address::from(*value.fee_recipient),
        state_root: value.state_root,
        transactions_root: txs_root,
        receipts_root: value.receipts_root,
        withdrawals_root: Some(withdrawals_root),
        difficulty: U256::ZERO,
        number: value.block_number,
        gas_limit: value.gas_limit,
        gas_used: value.gas_used,
        timestamp: value.timestamp,
        mix_hash: value.prev_randao,
        nonce: empty_nonce,
        base_fee_per_gas: Some(value.base_fee_per_gas.to::<u64>()),
        blob_gas_used: Some(value.blob_gas_used),
        excess_blob_gas: Some(value.excess_blob_gas),
        parent_beacon_block_root: None,
        extra_data: value.extra_data.to_vec().into(),
        requests_hash: None,
        logs_bloom,
    };

    let header = Header {
        hash: value.block_hash,
        inner: consensus_header,
        total_difficulty: Some(U256::ZERO),
        size: Some(U256::ZERO),
    };

    Ok(Block::new(header, BlockTransactions::Full(txs))
        .with_withdrawals(Some(Withdrawals::new(withdrawals))))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod recovery_tests {
    use super::*;
    use alloy::signers::{local::PrivateKeySigner, SignerSync};
    use ssz::Encode;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }
    fn signed(
        signer: &PrivateKeySigner,
        chain: u64,
        timestamp: u64,
        number: u64,
    ) -> SequencerCommitment {
        let payload = ExecutionPayload {
            parent_hash: B256::ZERO,
            fee_recipient: Address::ZERO,
            state_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: vec![0; 256].into(),
            prev_randao: B256::ZERO,
            block_number: number,
            gas_limit: 30_000_000,
            gas_used: 0,
            timestamp,
            extra_data: Vec::new().try_into().unwrap(),
            base_fee_per_gas: U256::from(1),
            block_hash: B256::ZERO,
            transactions: Vec::new().try_into().unwrap(),
            withdrawals: Vec::new().try_into().unwrap(),
            blob_gas_used: 0,
            excess_blob_gas: 0,
            withdrawals_root: B256::ZERO,
        };
        let data: alloy::primitives::Bytes = [vec![0; 32], payload.as_ssz_bytes()].concat().into();
        let signature = signer
            .sign_hash_sync(&crate::signature_msg(&data, chain))
            .unwrap();
        SequencerCommitment { data, signature }
    }

    async fn responder(
        body: String,
        status: &str,
        stall: bool,
    ) -> (Url, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/latest", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let response = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}",body.len());
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            let _ = stream.read(&mut buf).await;
            if stall {
                std::future::pending::<()>().await;
            }
            let _ = stream.write_all(response.as_bytes()).await;
        });
        (url, task)
    }

    #[tokio::test]
    async fn hung_request_is_bounded_and_next_request_recovers() {
        let http = reqwest::Client::new();
        let (url, task) = responder(String::new(), "200 OK", true).await;
        let err = fetch_commitment(&http, url, Duration::from_millis(30))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timed out"));
        task.abort();
        let signer = PrivateKeySigner::random();
        let c = signed(&signer, 84532, now(), 1);
        let (url, task) = responder(serde_json::to_string(&c).unwrap(), "200 OK", false).await;
        let c = fetch_commitment(&http, url, Duration::from_secs(2))
            .await
            .unwrap();
        c.verify(signer.address(), 84532).unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn reports_http_status_and_rejects_malformed_json() {
        for (body, status, expected) in [
            ("bad gateway", "502 Bad Gateway", "502"),
            ("null", "200 OK", "decoding"),
        ] {
            let (url, task) = responder(body.into(), status, false).await;
            let err = fetch_commitment(&reqwest::Client::new(), url, Duration::from_secs(2))
                .await
                .unwrap_err();
            assert!(err.to_string().contains(expected), "{err}");
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn only_fresh_correctly_signed_heads_make_consensus_ready() {
        let signer = PrivateKeySigner::random();
        let other = PrivateKeySigner::random();
        let (block_send, mut blocks) = channel(8);
        let (finalized_block_send, _) = watch::channel(None);
        let (accepted_send, accepted) = watch::channel(None);
        let (shutdown, shutdown_recv) = watch::channel(false);
        let mut inner = Inner {
            server_url: "http://127.0.0.1/".parse().unwrap(),
            unsafe_signer: Arc::new(Mutex::new(signer.address())),
            chain_id: 84532,
            latest_block: None,
            block_send,
            finalized_block_send,
            accepted_send,
            http: reqwest::Client::new(),
        };
        assert!(inner.accept(signed(&other, 84532, now(), 1)).await.is_err());
        assert!(inner.accept(signed(&signer, 1, now(), 1)).await.is_err());
        assert!(inner
            .accept(signed(&signer, 84532, now() - 61, 1))
            .await
            .is_err());
        assert!(inner
            .accept(signed(&signer, 84532, now() + 60, 1))
            .await
            .is_err());
        assert!(accepted.borrow().is_none());
        assert!(blocks.try_recv().is_err());
        assert!(wait_for_fresh_head(
            accepted.clone(),
            shutdown_recv.clone(),
            Duration::from_millis(20)
        )
        .await
        .is_err());
        inner
            .accept(signed(&signer, 84532, now(), 2))
            .await
            .unwrap();
        wait_for_fresh_head(accepted, shutdown_recv, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(blocks.recv().await.unwrap().header.number, 2);
        shutdown.send_replace(true);
    }

    #[tokio::test]
    async fn shutdown_interrupts_waiting_for_sync() {
        let (_tx, rx) = watch::channel(None);
        let (stop, stop_rx) = watch::channel(false);
        stop.send_replace(true);
        assert!(wait_for_fresh_head(rx, stop_rx, Duration::from_secs(2))
            .await
            .unwrap_err()
            .to_string()
            .contains("stopped"));
    }
}
