# Nova OP Stack recovery candidate

This branch targets the Helios revision pinned by Nova Capsule, starting from
`6c306c9b049773d6317521157dbb540895f32e7f`. It does not change a configured
sequencer signer, disable signature checks, or substitute execution RPC for a
signed-block feed. OP Stack's unsafe sequencer signature is not proof of L1
finality.

## Client behavior

- A signed-feed request, including body decoding, has a 10-second deadline.
- HTTP errors retain their status instead of appearing as JSON decode failures.
- Invalid signatures, malformed payloads and malformed transactions return errors.
- A head older than 60 seconds, or more than 5 seconds in the future, is rejected.
- `wait_synced` waits for a fresh verified payload; after 60 seconds it reports an
  error. Polling continues so a transient provider outage can recover. Callers
  can retry `wait_synced` on the same client; they must not treat a timeout as
  readiness or create multiple clients bound to the same RPC port.
- `shutdown` cancels OP Stack polling even during a hung request. This is not a
  claim that every Helios RPC-server task or optional Ethereum signer-refresh
  task has a complete shutdown lifecycle.
- The upstream closed-consensus-channel fix prevents the core's consumer loop
  from spinning when the producer exits.

## Relay experiment

Networking changes are based on upstream PR 801, revision
`69c09403ae7a3c2c6e2e1a46854a82fcd404559a`: V4 topics, yamux transport, identify,
and 10-MiB gossip framing. Source (https://github.com/a16z/helios/pull/801).
The PR remains unmerged upstream; taking its changes does not establish that a
relay works on today's Base Sepolia network.

Base Sepolia bootstrap ENRs are taken from Base v1.5.1; the old list contained
Base Goerli and mainnet bootnodes. Source
(https://github.com/base/base/blob/v1.5.1/crates/common/chains/src/config.rs).

Message IDs include the topic as in the current Base implementation. Snappy
lengths are bounded before decompression, short messages are rejected, and ENRs
without TCP ports are skipped. Source
(https://github.com/base/base/blob/v1.5.1/crates/consensus/gossip/src/config.rs).

The relay's HTTP listener should remain on loopback during evaluation. A future
public relay must serve only signed public block data. No KMS secret belongs
there. A failed or stale feed remains an availability failure; no fallback to
unsigned blocks is permitted.

## Validation commands

```sh
cargo test --locked -p helios-opstack --lib
cargo build --locked -p helios-opstack --bin opstack-server
cargo build --locked -p helios-opstack --example nova_registry_probe
```

Run the relay with `--network base-sepolia`, a loopback HTTP address, and a free
gossip port. The CLI currently also requires `--execution-rpc`, although the
existing relay entrypoint does not use that argument.

`nova_registry_probe SIGNED_FEED_URL` waits for verified sync and performs
read-only calls to Nova's original AppRegistry and KMSRegistry on Base Sepolia.
It does not load a wallet, send a transaction, or change KMS state. A successful
unit test is not a substitute for passing this live probe and an in-enclave
KMS test.
