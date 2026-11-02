use alloy::primitives::Address;
use libp2p::gossipsub::{IdentTopic, Message, MessageAcceptance, TopicHash};
use tokio::sync::mpsc::Sender;

use crate::SequencerCommitment;

pub struct BlockHandler {
    chain_id: u64,
    signer: Address,
    commitment_sender: Sender<SequencerCommitment>,
    blocks_v3_topic: IdentTopic,
    blocks_v4_topic: IdentTopic,
}

impl BlockHandler {
    pub fn new(signer: Address, chain_id: u64, sender: Sender<SequencerCommitment>) -> Self {
        Self {
            chain_id,
            signer,
            commitment_sender: sender,
            blocks_v3_topic: IdentTopic::new(format!("/optimism/{chain_id}/2/blocks")),
            blocks_v4_topic: IdentTopic::new(format!("/optimism/{chain_id}/3/blocks")),
        }
    }

    pub fn topics(&self) -> Vec<TopicHash> {
        vec![self.blocks_v3_topic.hash(), self.blocks_v4_topic.hash()]
    }

    pub fn handle(&self, msg: Message) -> MessageAcceptance {
        let commitment = match SequencerCommitment::new(&msg.data) {
            Ok(commitment) => commitment,
            Err(error) => {
                tracing::debug!(%error, bytes=msg.data.len(), topic=%msg.topic, "invalid gossip envelope");
                return MessageAcceptance::Reject;
            }
        };
        match commitment.verify(self.signer, self.chain_id) {
            Ok(()) => {
                tracing::debug!(bytes = commitment.data.len(), "accepted signed gossip");
                _ = self.commitment_sender.try_send(commitment);
                MessageAcceptance::Accept
            }
            Err(error) => {
                tracing::debug!(%error, "invalid sequencer gossip signature");
                MessageAcceptance::Reject
            }
        }
    }
}
