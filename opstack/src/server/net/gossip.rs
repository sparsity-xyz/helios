use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use eyre::Result;
use libp2p::{
    futures::StreamExt,
    gossipsub::{self, IdentTopic, Message, MessageId},
    identify,
    identity::Keypair,
    multiaddr::Protocol,
    noise, ping,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux, Multiaddr, Swarm, SwarmBuilder,
};
use sha2::{Digest, Sha256};
use tokio::select;

use super::{block_handler::BlockHandler, discovery};

/// OP Stack gossip service
pub struct GossipService {
    /// The socket address that the service is listening on.
    addr: SocketAddr,
    /// The chain ID of the network
    chain_id: u64,
    /// A unique keypair to validate the node's identity
    keypair: Option<Keypair>,
    /// Handler for the block
    block_handler: BlockHandler,
}

impl GossipService {
    /// Creates a new [Service]
    pub fn new(addr: SocketAddr, chain_id: u64, handler: BlockHandler) -> Self {
        Self {
            addr,
            chain_id,
            keypair: None,
            block_handler: handler,
        }
    }

    /// Sets the keypair for [Service]
    pub fn set_keypair(mut self, keypair: Keypair) -> Self {
        self.keypair = Some(keypair);
        self
    }

    /// Starts the Discv5 peer discovery & libp2p services
    /// and continually listens for new peers and messages to handle
    pub fn start(self) -> Result<()> {
        let keypair = self.keypair.unwrap_or_else(Keypair::generate_secp256k1);

        let mut swarm = create_swarm(keypair, &self.block_handler)?;
        let mut peer_recv = discovery::start(self.addr, self.chain_id)?;
        let multiaddr = socket_to_multiaddr(self.addr);

        swarm
            .listen_on(multiaddr)
            .map_err(|_| eyre::eyre!("swarm listen failed"))?;

        tokio::spawn(async move {
            loop {
                select! {
                    peer = peer_recv.recv() => {
                        match peer {
                            Some(peer) => {
                                tracing::debug!(%peer, "dialing discovered peer");
                                _ = swarm.dial(socket_to_multiaddr(peer));
                            }
                            None => { tracing::error!("peer discovery stopped"); break; }
                        }
                    },
                    event = swarm.select_next_some() => {
                        match event {
                            SwarmEvent::Behaviour(event) => event.handle(&mut swarm, &self.block_handler),
                            SwarmEvent::ConnectionEstablished { peer_id, .. } => tracing::info!(%peer_id, "gossip connection established"),
                            SwarmEvent::OutgoingConnectionError { error, .. } => tracing::debug!(%error, "gossip connection failed"),
                            _ => {}
                        }
                    },
                }
            }
        });

        Ok(())
    }
}

fn socket_to_multiaddr(socket: SocketAddr) -> Multiaddr {
    let mut multiaddr = Multiaddr::empty();
    match socket.ip() {
        IpAddr::V4(ip) => multiaddr.push(Protocol::Ip4(ip)),
        IpAddr::V6(ip) => multiaddr.push(Protocol::Ip6(ip)),
    }
    multiaddr.push(Protocol::Tcp(socket.port()));
    multiaddr
}

/// Computes the message ID of a `gossipsub` message
fn compute_message_id(msg: &Message) -> MessageId {
    let decoded = snap::raw::decompress_len(&msg.data)
        .ok()
        .filter(|len| *len <= 10 * 1024 * 1024)
        .and_then(|_| snap::raw::Decoder::new().decompress_vec(&msg.data).ok());
    let (domain, data): ([u8; 4], &[u8]) = match &decoded {
        Some(data) => ([1, 0, 0, 0], data),
        None => ([0, 0, 0, 0], &msg.data),
    };
    let topic = msg.topic.as_str().as_bytes();
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update((topic.len() as u64).to_le_bytes());
    hash.update(topic);
    hash.update(data);
    MessageId(hash.finalize()[..20].to_vec())
}

/// Creates the libp2p [Swarm]
fn create_swarm(keypair: Keypair, handler: &BlockHandler) -> Result<Swarm<Behaviour>> {
    Ok(SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )
        .map_err(|e| eyre::eyre!("tcp transport setup failed: {e}"))?
        .with_behaviour(|key| Behaviour::new(key, handler).expect("behaviour creation failed"))
        .map_err(|e| eyre::eyre!("behaviour setup failed: {e}"))?
        .build())
}

/// Specifies the [NetworkBehaviour] of the node
#[derive(NetworkBehaviour)]
#[behaviour(out_event = "Event")]
struct Behaviour {
    /// Adds [libp2p::ping] to respond to inbound pings, and send periodic outbound pings
    ping: ping::Behaviour,
    /// Adds [libp2p::gossipsub] to enable gossipsub as the routing layer
    gossipsub: gossipsub::Behaviour,
    /// Adds [libp2p::identify] so op-node peers can identify us. go-libp2p
    /// based op-nodes close connections within ~300ms when the identify
    /// protocol is unsupported, which starves the node of gossip mesh
    /// membership (blocks then only trickle in via rare opportunistic
    /// grafts). Mirrors kona's behaviour composition.
    identify: identify::Behaviour,
}

impl Behaviour {
    /// Configures the swarm behaviors, subscribes to the gossip topics, and returns a new [Behaviour]
    fn new(keypair: &Keypair, handler: &BlockHandler) -> Result<Self> {
        let ping = ping::Behaviour::default();

        let identify = identify::Behaviour::new(
            identify::Config::new(String::new(), keypair.public())
                .with_agent_version("helios".to_string()),
        );

        let gossipsub_config = gossipsub::ConfigBuilder::default()
            // OP spec gossip max: 10 MiB. The libp2p default (64 KiB) kills
            // the inbound gossipsub stream with a codec error on the first
            // Base block larger than 64 KiB, silencing the peer until it
            // reconnects — blocks then only arrive in short bursts.
            .max_transmit_size(10 * (1 << 20))
            .mesh_n(8)
            .mesh_n_low(6)
            .mesh_n_high(12)
            .gossip_lazy(6)
            .heartbeat_interval(Duration::from_millis(500))
            .fanout_ttl(Duration::from_secs(24))
            .history_length(12)
            .history_gossip(3)
            .duplicate_cache_time(Duration::from_secs(65))
            .validation_mode(gossipsub::ValidationMode::None)
            .validate_messages()
            .message_id_fn(compute_message_id)
            .build()
            .map_err(|_| eyre::eyre!("gossipsub config creation failed"))?;

        let mut gossipsub =
            gossipsub::Behaviour::new(gossipsub::MessageAuthenticity::Anonymous, gossipsub_config)
                .map_err(|_| eyre::eyre!("gossipsub behaviour creation failed"))?;

        handler
            .topics()
            .iter()
            .map(|topic| {
                let topic = IdentTopic::new(topic.to_string());
                gossipsub
                    .subscribe(&topic)
                    .map_err(|_| eyre::eyre!("subscription failed"))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            ping,
            gossipsub,
            identify,
        })
    }
}

/// The type of message received
enum Event {
    /// Represents a [ping::Event]
    #[allow(dead_code)]
    Ping(ping::Event),
    /// Represents a [gossipsub::Event]
    Gossipsub(gossipsub::Event),
    /// Represents an [identify::Event]
    #[allow(dead_code)]
    Identify(identify::Event),
}

impl Event {
    /// Handles received gossipsub messages. Ping messages are ignored.
    /// Reports back to [libp2p::gossipsub] to apply peer scoring and forward the message to other peers if accepted.
    fn handle(self, swarm: &mut Swarm<Behaviour>, handler: &BlockHandler) {
        if let Self::Gossipsub(gossipsub::Event::Message {
            propagation_source,
            message_id,
            message,
        }) = self
        {
            let status = handler.handle(message);

            _ = swarm
                .behaviour_mut()
                .gossipsub
                .report_message_validation_result(&message_id, &propagation_source, status);
        }
    }
}

impl From<ping::Event> for Event {
    /// Converts [ping::Event] to [Event]
    fn from(value: ping::Event) -> Self {
        Event::Ping(value)
    }
}

impl From<gossipsub::Event> for Event {
    /// Converts [gossipsub::Event] to [Event]
    fn from(value: gossipsub::Event) -> Self {
        Event::Gossipsub(value)
    }
}

impl From<identify::Event> for Event {
    /// Converts [identify::Event] to [Event]
    fn from(value: identify::Event) -> Self {
        Event::Identify(value)
    }
}

#[cfg(test)]
mod message_id_tests {
    use super::*;
    #[test]
    fn matches_base_reference_and_separates_topics() {
        let mut message = Message {
            source: None,
            data: snap::raw::Encoder::new()
                .compress_vec(&[1, 2, 3, 4, 5])
                .unwrap(),
            sequence_number: None,
            topic: gossipsub::TopicHash::from_raw("test"),
        };
        assert_eq!(
            hex::encode(compute_message_id(&message).0),
            "adbe547b27f41294a08a09210a5e8531e83cdc16"
        );
        let first = compute_message_id(&message);
        message.topic = gossipsub::TopicHash::from_raw("/optimism/84532/3/blocks");
        assert_ne!(first, compute_message_id(&message));
    }
}
