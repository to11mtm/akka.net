# Akka.Remote — Pipe Transport Zero-Copy Redesign: Implementation Checklist ✨ (uwu edition)

> **Purpose:** Track implementation of the zero-copy redesign proposed in
> [`akka-pipe-transport-zero-copy-redesign.md`](./akka-pipe-transport-zero-copy-redesign.md).
> Tick items as PRs land; this doc is the single source of truth for
> "what's done, what's next."
>
> **Companion docs:**
> - [`akka-pipe-transport-copy-analysis.md`](./akka-pipe-transport-copy-analysis.md) — copy/alloc audit (the "before" picture)
> - [`akka-pipe-transport-zero-copy-redesign.md`](./akka-pipe-transport-zero-copy-redesign.md) — the proposal we're executing
> - [`akka-remote-akka-protocol-redux.md`](./akka-remote-akka-protocol-redux.md) — inline FSM design used by PR-C
>
> **Status legend:**
> - `[ ]` not started
> - `[~]` in progress (PR open)
> - `[x]` merged
> - `[!]` blocked / deferred (note inline)
>
> **Tracking:** record PR numbers next to each top-level task as they
> open, e.g. `(#7432)`. Update the "PR registry" at the bottom too.

---

## PR-A — Hand-written zero-copy codec (no behavioural change)

**Goal:** eliminate `R2`, `R3`, `W4`, `W5` while keeping wire bytes
identical. Gated behind `akka.remote.pipe.tcp.zero-copy-codec`
(default `off` in the first release).

### A.1 — Hand-written outer envelope codec (§3.2, §3.3)

- [x] **`OuterEnvelopeWriter`** — emit `AkkaProtocolMessage{Payload}` & `{Instruction}` wire bytes directly into an `IBufferWriter<byte>`
  - [x] `Transport/Pipelines/Codec/OuterEnvelopeWriter.cs` — `WritePayloadFrame`, `WriteControlFrame`
  - [x] Varint encode helper (`ComputeVarintSize` / `WriteVarint`) in `Transport/Pipelines/Codec/ProtobufWire.cs`
  - [x] Unit tests: every byte sequence matches `new AkkaProtocolMessage { Payload = … }.ToByteString().ToByteArray()`
- [x] **`OuterEnvelopeReader`** — zero-copy slice `ReadOnlySequence<byte>` → `(FrameKind, body)`
  - [x] `Transport/Pipelines/Codec/OuterEnvelopeReader.cs` — `TryRead(ref ReadOnlySequence<byte>, out FrameKind, out ReadOnlySequence<byte>)`
  - [x] Varint decode helper (`TryReadVarint`)
  - [x] Unit tests: differential vs. `AkkaProtocolMessage.Parser.ParseFrom` over fuzzed inputs (42 samples; expand to 10k in X.2)

### A.2 — Hand-written inner envelope codec (§3.4)

- [x] **`InnerEnvelopeWriter`** — emit `AckAndEnvelopeContainer` wire bytes
  - [x] `Transport/Pipelines/Codec/InnerEnvelopeWriter.cs` — `WriteAckAndEnvelope(writer, recipientPath, senderPath, seq, ack?, msgBody, manifest, serId)`
  - [x] Reuse `ProtobufWire` varint helpers
  - [x] Unit tests: byte-identity vs. `AckAndEnvelopeContainer{…}.ToByteString()`
- [x] **`InnerEnvelopeReader`** — zero-copy slice into `DecodedInnerEnvelope` struct
  - [x] `Transport/Pipelines/Codec/InnerEnvelopeReader.cs`
  - [x] `DecodedInnerEnvelope` struct with `ReadOnlySequence<byte>` zero-copy fields (no heap alloc on hot path)
  - [x] Unit tests: differential vs. `AckAndEnvelopeContainer.Parser.ParseFrom`

### A.3 — Pooled output frame plumbing (§6)

- [x] **`IPooledFrame`** + `PooledFrame` impl wrapping an `IMemoryOwner<byte>` and `WrittenSpan`
  - [x] `Transport/Pipelines/PooledFrame.cs` — `IPooledFrame`, `PooledFrame` (pool-rented), `ByteStringPooledFrame` (legacy wrapper)
  - [x] `Dispose()` returns owner to `MemoryPool<byte>.Shared`
- [x] **`PipeAssociationHandle.Write(ByteString)`** dual-path
  - [x] Legacy path (flag off): wraps `ByteString` in `ByteStringPooledFrame`, enqueues unchanged
  - [x] Zero-copy path: `WriteRaw(ReadOnlyMemory<byte>)` rents `PooledFrame`, calls `OuterEnvelopeWriter.WritePayloadFrame`, enqueues frame
- [x] **`PipeConnection`** channel type changed to `Channel<IPooledFrame>` (both paths)
  - [x] `WriteLoopAsync` adapted to drain `IPooledFrame` and dispose after copy into batch
  - [x] `TryEnqueueWrite(IPooledFrame)` overload added; legacy `TryEnqueueWrite(ByteString)` delegates
- [!] **Back-patch trick** for outer length varint (§6.1) — deferred; pre-compute-size approach used instead (equivalent performance, simpler code)

### A.4 — HOCON wiring

- [x] `akka.remote.pipe.tcp.zero-copy-codec` config key (default `off`)
- [x] `PipeTransportSettings.ZeroCopyCodec` property reads the flag
- [x] `TcpPipeTransport` passes `zeroCopyCodec` flag when constructing `PipeAssociationHandle`
- [x] `AkkaProtocolHandle.Write` checks `PipeAssociationHandle.IsZeroCopyEnabled` and routes to `WriteRaw`
- [x] Reference config + xmldoc updated

### A.5 — `CodecSchemaGuardSpec` (Risk #1, §10)

- [x] `Akka.Remote.Tests/Transport/Pipelines/CodecSchemaGuardSpec.cs`
  - [x] Reflects over `AkkaProtocolMessage.Descriptor`, `AckAndEnvelopeContainer.Descriptor`, `RemoteEnvelope.Descriptor`, `AcknowledgementInfo.Descriptor`, `Payload.Descriptor`, `ActorRefData.Descriptor`
  - [x] Asserts field numbers, wire types, and names match the constants in the hand-written codec
  - [x] Test fails loudly with "Edit InnerEnvelopeWriter.cs / OuterEnvelopeWriter.cs to use 0xXX" hint if the proto changes

### A.6 — PR-A definition of done

- [x] All A.1–A.5 boxes ticked
- [x] All existing `AkkaProtocolSpec` tests pass with flag off (default)
- [x] New codec unit tests: 84 tests, 0 failures
- [ ] Wire-format snapshot tests (see §X.1) — deferred to X.1 cross-cutting work
- [x] No public API changes
- [ ] PR number recorded: `_____`

---

## PR-B — Pooled inbound payload + serializer overrides

**Goal:** eliminate `PR1` allocation (becomes a pool rent) and the
`R4-fallback` copy for core serializers. Additive — no flag required.

### B.1 — `IPooledInboundPayload` SPI (§4)

- [ ] `Akka.Remote/Transport/IPooledInboundPayload.cs`
  - [ ] `interface IPooledInboundPayload : IDisposable { ReadOnlyMemory<byte> Memory; ReadOnlySpan<byte> Span; int Length; }`
- [ ] `RentedInboundPayload` — wraps `IMemoryOwner<byte>` from `MemoryPool<byte>.Shared`
- [ ] `SegmentAliasPayload` — zero-copy alias over a single `PipeReader` segment; `Dispose` is a no-op; `DEBUG`-only escape detection
- [ ] `InboundPayload` gains `public IPooledInboundPayload? Pooled { get; init; }` and lazily materialises `Payload` (`ByteString`) on access for legacy consumers

### B.2 — `PipeConnection` produces pooled payloads

- [ ] `ReadLoopAsync` constructs `SegmentAliasPayload` for fast-path single-segment frames
- [ ] Multi-segment frames promoted to `RentedInboundPayload` via pool rent + single memcpy (replaces `frame.ToArray()`)
- [ ] Hand-off to `EndpointReader` always uses `RentedInboundPayload` (mailbox crossing — see §4.3)
- [ ] Counting `MemoryPool<byte>` test fixture asserts every rent has a matching return

### B.3 — `Serializer.FromBinary(ReadOnlyMemory<byte>, …)` overrides

- [ ] `NewtonSoftJsonSerializer` — read directly from `ReadOnlyMemory<byte>` (UTF-8 → `JsonTextReader` over `MemoryStream` wrapping the rented memory)
- [ ] `HyperionSerializer` — `Hyperion.Serializer.Deserialize` from a `ReadOnlyMemoryStream` wrapper
- [ ] `ByteArraySerializer` — return `memory.ToArray()` only when caller needs ownership; otherwise return `memory` as-is where API permits
- [ ] `ProtobufSerializer` (contrib + core protobuf-backed serializers like system message serializer, daemon message serializer) — use `MessageParser.ParseFrom(ReadOnlySpan<byte>)`
- [ ] Per-serializer unit test confirms no `ToArray()` allocation on the hot path (allocation-counting assertion)

### B.4 — `Serializer.ToBinary(IBufferWriter<byte>, object)` additive virtual

- [ ] `Akka/Serialization/Serializer.cs` — add `public virtual void ToBinary(IBufferWriter<byte> writer, object obj)` with default impl calling `ToBinary(obj)` + `writer.Write(bytes)`
- [ ] Override on core serializers from B.3 where the underlying library supports streaming write (`HyperionSerializer`, protobuf-based)
- [ ] `NewtonSoftJsonSerializer` — write through `Utf8JsonWriter`-style streaming if/when migrating; otherwise keep default
- [ ] Per-serializer unit test confirms write goes straight to the buffer writer

### B.5 — `MessageSerializer` & `EndpointReader` consume the new APIs

- [ ] `EndpointReader.OnInboundPayload` checks `Pooled` first; falls back to `Payload`
- [ ] `MessageSerializer.Deserialize(ReadOnlyMemory<byte>, …)` already exists — no change needed
- [ ] `MessageSerializer.Serialize` gains overload that writes into an `IBufferWriter<byte>` (used by PR-A inner writer)

### B.6 — PR-B definition of done

- [ ] All B.1–B.5 boxes ticked
- [ ] Allocation benchmark: `RoundTrip_1KiB` shows ≥1 fewer `byte[]` alloc per op
- [ ] Pool-leak test: 100k round-trips, zero net `IMemoryOwner` outstanding at end
- [ ] All existing `Akka.Remote.Tests` + `Akka.Cluster.Tests` green
- [ ] PR number recorded: `_____`

---

## PR-C — Inline state machine (per redux doc)

**Goal:** eliminate the `ProtocolStateActor` mailbox hop per frame.
Gated behind `akka.remote.pipe.tcp.inline-protocol` (default `off`).

### C.1 — `InlineProtocolState` core

- [ ] `Transport/Pipelines/InlineProtocolState.cs`
  - [ ] `enum InlinePhase { WaitHandshake, Open, Closed }`
  - [ ] `struct InlineProtocolState` carrying phase, peer `HandshakeInfo`, refuseUid, FD ref, listener ref, pre-listener buffer
- [ ] `OnFrame(FrameKind, ReadOnlySequence<byte>)` dispatch — called synchronously from `PipeConnection.ReadLoopAsync`
- [ ] Inline handshake (Associate exchange)
- [ ] Inline Disassociate handling — produces `AkkaProtocolException` text byte-identical to FSM today

### C.2 — Shared heartbeat tick

- [ ] One `PeriodicTimer` per `PipeTransport` (not per association)
- [ ] On tick: walk active connections, call `_failureDetector.HeartBeat()` if no recv since last tick, enqueue `Heartbeat` frame
- [ ] Handshake timeout enforced via `Stopwatch.GetTimestamp()` checked from tick (no per-association scheduler entry)

### C.3 — Pre-listener bounded buffer (Risk #2 from redux doc)

- [ ] `InlineProtocolState` buffers up to N (config; default 32) frames between `Open` and `SetListener`
- [ ] Buffer overflow → hard disassociate with `AkkaProtocolException("pre-listener buffer overflow")`
- [ ] Release note item added to `RELEASE_NOTES.md` draft

### C.4 — `AkkaProtocolHandle` facade routing

- [ ] When `inline-protocol = on`, `PipeTransport` constructs handles directly (skips `AkkaProtocolTransport` adapter)
- [ ] `AkkaProtocolHandle.Disassociate(info)` routes to `InlineProtocolState.DisassociateInline` (not actor `Tell`)
- [ ] `EndpointManager` / `EndpointWriter` see the same `AssociationHandle` SPI — no upstream changes

### C.5 — Behaviour preservation (from redux doc §"Behaviours that MUST be preserved exactly")

- [ ] Quarantine: `DisassociateQuarantined` produces same `AkkaProtocolException` text
- [ ] `RefuseUid`: outbound match → `SendDisassociate(Quarantined)` + `ForbiddenUidReason` text identical
- [ ] Handshake timeout: same `AkkaProtocolException` + `TimeoutException` propagation
- [ ] Disassociate logging strings match FSM emissions

### C.6 — HOCON & migration

- [ ] `akka.remote.pipe.tcp.inline-protocol` config key (default `off`)
- [ ] When `on`, skip `AkkaProtocolTransport` registration in `PipeTransport` setup
- [ ] Reference config + xmldoc + release note

### C.7 — PR-C definition of done

- [ ] All C.1–C.6 boxes ticked
- [ ] `AkkaProtocolSpec` + `AkkaProtocolStressTest` pass with flag both `on` and `off`
- [ ] Inline state machine focused unit suite (covers every FSM transition)
- [ ] Diagnostics replacement (structured `Akka.Remote.InlineProtocol.Trace` event source) added — Risk #5 from redux doc
- [ ] PR number recorded: `_____`

---

## Cross-cutting — tests, benchmarks, docs

### X.1 — Wire-format snapshot tests (§8.1)

- [ ] `Akka.Remote.Tests/Transport/Pipelines/WireFormat.snapshots/` directory created
- [ ] Snapshot generator captures bytes for every PDU shape:
  - [ ] `Associate` (with HandshakeInfo)
  - [ ] `Disassociate` / `DisassociateShuttingDown` / `DisassociateQuarantined`
  - [ ] `Heartbeat`
  - [ ] `Payload(empty inner)`
  - [ ] `Payload(envelope without ack)`
  - [ ] `Payload(envelope with ack)`
  - [ ] `Payload(envelope + system message)`
- [ ] Each snapshot test asserts byte-identity for legacy vs. new codec output
- [ ] Each snapshot decoded by both legacy `Parser.ParseFrom` and new reader; structural equality asserted

### X.2 — Differential decode fuzz test (§8.2)

- [ ] `DifferentialDecodeSpec` — generates 10 000 randomised valid PDUs via legacy serializer
- [ ] Decodes via both legacy parser and new readers; field-level equality
- [ ] Seeded RNG; corpus committed under `Tests/Transport/Pipelines/fuzz-corpus/`

### X.3 — Interop matrix (§8.3)

- [ ] `Akka.Remote.Tests/Transport/Pipelines/InteropSpec.cs` — multi-node spec
  - [ ] Legacy sender ↔ New receiver
  - [ ] New sender ↔ Legacy receiver
  - [ ] Legacy ↔ Legacy (sanity)
  - [ ] New ↔ New
- [ ] All four run `AkkaProtocolSpec` + `AkkaProtocolStressTest` payloads

### X.4 — Lifetime / pool tests (§8.4)

- [ ] `CountingMemoryPool<byte>` test fixture (instrumented `MemoryPool<byte>`)
- [ ] Asserts every rent matched by return after `EndpointReader` dispatch
- [ ] Tiny-segment PipeReader test forces multi-segment frames; asserts no `SegmentAliasPayload` escapes the read-loop iteration

### X.5 — Benchmark gates (§8.5)

- [ ] `src/benchmark/Akka.Remote.Benchmarks/PipeTransportZeroCopyBenchmarks.cs`
- [ ] Benchmarks:
  - [ ] `RoundTrip_1KiB` — allocations/op target ≤2 (vs. ~6 baseline)
  - [ ] `RoundTrip_64KiB` — Gen0 collections per 100k ops near zero
  - [ ] `Throughput_Tell_1KiB` — msgs/sec ≥ 1.5× baseline
- [ ] CI gate: fail on >5 % regression vs. baseline JSON committed in repo

### X.6 — Copy-elimination scoreboard (§12 verification)

Tick when benchmark + code review confirm the copy is gone (or the
fallback path is the only remaining reference):

- [ ] **PR1** — kernel→ByteString alloc replaced by pool rent (PR-B)
- [ ] **R2** — outer protobuf `bytes Payload` copy eliminated (PR-A) ✅ *infrastructure done; wired in PR-C*
- [ ] **R3** — inner protobuf `bytes Message` copy eliminated (PR-A) ✅ *infrastructure done; wired in PR-C*
- [ ] **R4-fallback** — eliminated for `NewtonSoftJson`, `Hyperion`, `ByteArray`, `Protobuf` (PR-B)
- [ ] **R5** — *[out of scope — follow-up ticket]*
- [ ] **W1** — eliminated for serializers overriding `ToBinary(IBufferWriter)` (PR-B)
- [ ] **W3** — *[out of scope — follow-up ticket]*
- [ ] **W4** — inner `ToByteString()` allocation gone (PR-A) ✅ *infrastructure done; `InnerEnvelopeWriter` wired in PR-B*
- [x] **W5** — outer `ToByteString()` allocation gone (PR-A) ✅ *`AkkaProtocolHandle.Write` → `WriteRaw` when `zero-copy-codec=on`*
- [ ] **PW8** — intentionally retained (write-coalescing)
- [ ] **ProtocolStateActor mailbox hop** — gone for hot frames (PR-C)

### X.7 — Documentation

- [ ] xmldoc on every new public/internal type (`IPooledInboundPayload`, `OuterEnvelopeWriter`, etc.)
- [ ] `docs/articles/remoting/` — add a "Pipe transport: zero-copy mode" article describing the two HOCON flags
- [ ] `RELEASE_NOTES.md` entry per PR (A/B/C)
- [ ] Update `akka-pipe-transport-copy-analysis.md` §"Already-fixed Copies" table once each copy lands

### X.8 — Out-of-scope follow-ups (file as separate tickets when checklist closes)

- [ ] Manifest interning cache (kills `R5` + `W3`)
- [ ] `IActorRef → ActorRefData` LRU cache on `EndpointWriter`
- [ ] Phase 2 framing-tag fast path (1-byte tag prefix; needs both-peer negotiation)
- [ ] `EndpointReader` collapse onto read loop (kills the surviving `PR1` copy)
- [ ] MessagePack envelope codec (separate proposal)
- [ ] Artery-style outbound stream replacing `EndpointWriter` / `ReliableDeliverySupervisor`

---

## Final "done" criteria

The redesign is **complete** when all of the following hold:

- [ ] PR-A, PR-B, PR-C all merged and at default `on` for at least one minor release
- [ ] Legacy `AkkaPduProtobuffCodec` POCO-based path marked `[Obsolete]`
- [ ] `ProtocolStateActor` + `AkkaProtocolManager` + `AkkaProtocolTransport` marked `[Obsolete]` for the pipe transport path
- [ ] All scoreboard items in §X.6 ticked
- [ ] All benchmark gates (§X.5) green for two consecutive minor releases
- [ ] Out-of-scope items in §X.8 filed as issues with links back to this checklist

---

## PR registry

| PR # | Scope | Status | Notes |
|------|-------|--------|-------|
| _____ | PR-A — zero-copy codec | `[ ]` | |
| _____ | PR-B — pooled inbound + serializer overrides | `[ ]` | |
| _____ | PR-C — inline state machine | `[ ]` | |

---

*Checklist generated May 2026. Update freely as work progresses. UwU~ 🌸*

