# Akka.Remote — Pipe Transport: Payload Copy Analysis ✨ (uwu edition)

> **Scope:** The full inbound (read) and outbound (write) path of the
> `TcpPipeTransport` — from the kernel socket to user-land deserialized
> object, and back again. This document traces **every copy of user-payload
> bytes**, noting the exact method, file, and copy type (allocation vs.
> memcpy), and records which copies have already been eliminated vs. which
> remain for the forthcoming redesign.
>
> **Analysed as of: May 2026 / `dev` branch.**
>
> **Key source files:**
> - `Transport/Pipelines/PipeConnection.cs` — read/write loops
> - `Transport/Pipelines/PipeAssociationHandle.cs` — write enqueue
> - `Transport/AkkaProtocolTransport.cs` — `ProtocolStateActor` (FSM)
> - `Transport/AkkaPduCodec.cs` — `AkkaPduProtobuffCodec` (codec)
> - `MessageSerializer.cs` — serialization bridge
> - `Endpoint.cs` — `EndpointWriter`, `EndpointReader`, `DefaultMessageDispatcher`
> - `Akka/Serialization/Serializer.cs` — base serializer API
>
> **Note:** The MessagePack codec (`AkkaPduMessagePackCodec`) is currently
> commented-out in `PipeTransportSettings.CreateCodec`. All analysis below
> uses the default Protobuf codec path.
>
> <!-- CopilotNotes: Cross-reference with the three sibling docs for deeper
>      analysis of specific sub-problems:
>      - akka-remote-read-pipeline-allocations.md  (DotNetty-centric + §7 Pipe)
>      - akka-remote-write-pipeline-allocations.md (DotNetty-centric + §7 Pipe)
>      - akka-remote-akka-protocol-redux.md        (Phase 2: inline FSM design)
>      This document is the definitive *current-state* reference for the
>      pipe transport specifically, updated to reflect fixes already applied. -->

---

## 1. Layered Architecture Overview

Before diving into copies, it helps to see which layer introduces which
abstraction. Every layer boundary is a potential copy point because the
public API between layers speaks `ByteString` — an **immutable, value-type
wrapper over `byte[]`** — which has no lifecycle hook and therefore cannot
alias a pooled buffer safely across a layer boundary.

```
┌────────────────────────────────────────────────────────────────────┐
│  Layer 5 — Kernel / OS                                             │
│    recv() / send()  via  NetworkStream.ReadAsync / WriteAsync      │
└─────────────────────────────────┬──────────────────────────────────┘
                                  │ ReadOnlyMemory<byte> (pooled PipeReader segments)
                                  ▼
┌────────────────────────────────────────────────────────────────────┐
│  Layer 4 — PipeConnection  (Transport SPI implementation)          │
│    PipeConnection.ReadLoopAsync / WriteLoopAsync / WriteFrame       │
│    PipeAssociationHandle.Write / TryEnqueueWrite                   │
│    API contract with layer above: ByteString  (InboundPayload)     │
└─────────────────────────────────┬──────────────────────────────────┘
                                  │ ByteString (InboundPayload actor message)
                                  ▼ [mailbox hop — actor message]
┌────────────────────────────────────────────────────────────────────┐
│  Layer 3 — ProtocolStateActor  (AkkaProtocol FSM)                  │
│    AkkaProtocolTransport.ProtocolStateActor                        │
│    AkkaPduProtobuffCodec.DecodePdu / ConstructPayload              │
│    API contract with layer above: ByteString  (InboundPayload)     │
└─────────────────────────────────┬──────────────────────────────────┘
                                  │ ByteString (InboundPayload actor message)
                                  ▼ [mailbox hop — actor message]
┌────────────────────────────────────────────────────────────────────┐
│  Layer 2 — EndpointReader / EndpointWriter  (Endpoint actors)      │
│    Endpoint.cs  EndpointReader.Reading / TryDecodeMessageAndAck    │
│    Endpoint.cs  EndpointWriter.WriteSend / SerializeMessage        │
│    AkkaPduProtobuffCodec.DecodeMessage / ConstructMessage          │
│    MessageSerializer.Serialize / Deserialize                       │
└─────────────────────────────────┬──────────────────────────────────┘
                                  │ object (deserialized user message)
                                  ▼
┌────────────────────────────────────────────────────────────────────┐
│  Layer 1 — User Code (actor)                                       │
│    IActorRef.Tell(message) / Receive<T>(handler)                   │
└────────────────────────────────────────────────────────────────────┘
```

**The fundamental problem:** Every "API contract" in the diagram above says
`ByteString`. `ByteString` is immutable, has no `Dispose` / `Release`, and
cannot safely alias memory that is owned and will eventually be freed by
another subsystem (e.g., a pooled `PipeReader` segment). So every layer
boundary currently copies the payload bytes into a freshly-allocated `byte[]`
to guarantee the `ByteString` lives indefinitely. With five such boundaries
between kernel and user code, we accumulate several full-payload copies per
message. UwU, that's a lot of memcpy~ 😿

---

## 2. Read Path: Kernel → User Actor

### 2.1 End-to-end diagram

```
 Kernel recv()
      │
      ▼  [already pooled — PipeReader rents from MemoryPool<byte>.Shared ✅]
 ReadOnlySequence<byte>  (one or more pooled segments)
      │
      │  PipeConnection.TryParseFrame(ref buffer, out frame)
      │    → zero-copy slice of the pooled sequence ✅
      │
      ▼
 ReadOnlySequence<byte> frame
      │
      │  PipeConnection.ReadLoopAsync
      │    ├─ fast path (single segment):
      │    │    ByteString.CopyFrom(frame.FirstSpan)                  ◀── COPY #PR1
      │    └─ slow path (multi-segment):
      │         UnsafeByteOperations.UnsafeWrap(frame.ToArray())      ◀── ALLOC #PR1b
      │         (frame.ToArray() = 1 alloc + 1 memcpy; UnsafeWrap = zero copy)
      │
      │  listener.Notify(new InboundPayload(bytes))
      ▼  [actor mailbox hop → ProtocolStateActor]
 InboundPayload.Payload : ByteString
      │
      │  ProtocolStateActor.Open case InboundPayload
      │    DecodePdu(ip.Payload)
      │      AkkaPduProtobuffCodec.DecodePdu(ByteString raw)
      │        AkkaProtocolMessage.Parser.ParseFrom(raw)              ◀── COPY #R2
      │          (protobuf ReadBytes() for 'bytes Payload' field
      │           allocates new byte[] + memcpy)
      │      new Payload(pdu.Payload)
      │        Payload.Bytes = pdu.Payload.Memory  ← zero copy ✅
      │    UnsafeByteOperations.UnsafeWrap(p.Bytes)  ← zero copy ✅
      │    lr.Listener.Notify(new InboundPayload(...))
      ▼  [actor mailbox hop → EndpointReader]
 InboundPayload.Payload : ByteString  (wraps same memory as Payload.Bytes)
      │
      │  EndpointReader.TryDecodeMessageAndAck(payload.Payload)
      │    AkkaPduProtobuffCodec.DecodeMessage(ByteString raw, ...)
      │      AckAndEnvelopeContainer.Parser.ParseFrom(raw)            ◀── COPY #R3
      │        (protobuf ReadBytes() for 'bytes Message' field
      │         allocates new byte[] + memcpy — this IS the user payload)
      │      envelopeContainer.Message  ← SerializedMessage POCO
      │        .Message : ByteString  (fresh byte[] from ParseFrom)
      │
      │  DefaultMessageDispatcher.Dispatch(recipient, addr, message, sender)
      │    MessageSerializer.Deserialize(system, messageProtocol)
      │      system.Serialization.Deserialize(
      │          messageProtocol.Message.Memory,   ← zero copy ✅ (ReadOnlyMemory)
      │          messageProtocol.SerializerId,
      │          messageProtocol.MessageManifest.ToStringUtf8())      ◀── COPY #R5 (manifest)
      │        serializer.FromBinary(bytes, manifest)
      │          if serializer does NOT override ReadOnlyMemory overload:
      │            bytes.ToArray()                                     ◀── COPY #R4-fallback
      │              → byte[] handed to serializer's inner logic
      ▼
 object (deserialized user message) → IActorRef mailbox
```

### 2.2 Read copies — current state table

| ID | Status | Location | Method / Call | Description |
|----|--------|----------|---------------|-------------|
| **#PR1** | ❌ **remains** | `PipeConnection.ReadLoopAsync` | `ByteString.CopyFrom(frame.FirstSpan)` | Full PDU copy from pooled PipeReader buffer into new `byte[]`. Required because `ByteString` cannot alias a `PipeReader`-owned segment that will be returned on `AdvanceTo`. |
| **#PR1b** | ⚠️ **improved** | `PipeConnection.ReadLoopAsync` | `frame.ToArray()` (slow path, multi-segment) | One alloc + one memcpy to produce a contiguous array; then wrapped zero-copy with `UnsafeWrap`. Previously was TWO copies. Occurs whenever a frame spans a PipeReader segment boundary (common for frames > 64 KiB, or under TLS). |
| **#R2** | ❌ **remains** | `AkkaPduProtobuffCodec.DecodePdu` | `AkkaProtocolMessage.Parser.ParseFrom(raw)` | Google.Protobuf's `CodedInputStream.ReadBytes()` for the inner `bytes Payload` field copies the payload into a fresh `byte[]`. The outer `AkkaProtocolMessage` envelope itself is tiny; the copy is of the full inner payload. |
| **#R3** | ❌ **remains** | `AkkaPduProtobuffCodec.DecodeMessage` | `AckAndEnvelopeContainer.Parser.ParseFrom(raw)` | Same protobuf `ReadBytes()` copy for the inner `bytes Message` field, which is the actual user-serialized payload. This is the copy that hurts most — it IS the user message bytes. |
| **#R4-fallback** | ⚠️ **infrastructure fixed, fallback remains** | `Serializer.FromBinary(ReadOnlyMemory<byte>, ...)` (base class) | `bytes.ToArray()` | `Serialization.Deserialize(ReadOnlyMemory<byte>, ...)` now exists and is called correctly by `MessageSerializer.Deserialize`. However the **base `Serializer.FromBinary(ReadOnlyMemory<byte>, Type)`** virtual default calls `bytes.ToArray()`, so every serializer that does not override the `ReadOnlyMemory` overload still copies here. Serializers that DO override it can skip this copy. |
| **#R5** | ❌ **remains (small)** | `MessageSerializer.Deserialize` | `messageProtocol.MessageManifest.ToStringUtf8()` | UTF-8 decode of the manifest `ByteString` into a `string`. Small (tens of bytes) but per-message. |
| ~~#R0~~ | ✅ **eliminated** | `PipeReader.Create(stream, ...)` | recv buffer | `PipeReader` rents segments from `MemoryPool<byte>.Shared` — no per-recv `byte[]` allocation (unlike DotNetty's `UnpooledByteBufferAllocator`). |
| ~~#R4~~ | ✅ **fixed at call site** | `MessageSerializer.Deserialize` | `messageProtocol.Message.Memory` | Previously called `messageProtocol.Message.ToByteArray()`. Now correctly passes `ReadOnlyMemory<byte>` to the `Serialization.Deserialize` overload. The copy only occurs in serializers that haven't yet overridden `FromBinary(ReadOnlyMemory<byte>, ...)`. |

**Net read-path user-payload copies:** PR1 + R2 + R3 + (R4-fallback for non-optimised serializers) = **3–4 full copies** from kernel recv to user serializer input.

> **CopilotNotes:** The chain is: recv buffer → ByteString (PR1) → 
> AkkaProtocolMessage inner Payload ByteString (R2) → 
> AckAndEnvelopeContainer inner Message ByteString (R3) → 
> byte[] inside serializer (R4-fallback for most serializers).
> Copies R2 and R3 are both attributable to the nested protobuf encoding:
> **two layers of protobuf wrapping** mean two layers of `ParseFrom` each
> of which re-copies the inner `bytes` field. This is the core inefficiency
> the redesign must address. 🌸

---

## 3. Write Path: User Actor → Kernel send()

### 3.1 End-to-end diagram

```
 IActorRef.Tell(message)  [user code, any thread]
      │
      ▼  [actor mailbox → EndpointWriter]
 EndpointWriter.WriteSend(Send send)
      │
      │  SerializeMessage(send.Message)
      │    MessageSerializer.Serialize(system, transportInfo, message)
      │      serializer.ToBinary(message)                             ◀── ALLOC #W1
      │        → fresh byte[] (unavoidable; serializer API contract)
      │      UnsafeByteOperations.UnsafeWrap(byte[])   ← zero copy ✅
      │        → SerializedMessage.Message : ByteString (wraps W1 array)
      │      ByteString.CopyFromUtf8(manifest)                       ◀── COPY #W3 (small)
      │        → SerializedMessage.MessageManifest : ByteString
      │
      │  pdu = _codec.ConstructMessage(recipient, addr,
      │            serializedMessage, sender, seq, lastAck)
      │    AkkaPduProtobuffCodec.ConstructMessage(...)
      │      new AckAndEnvelopeContainer (+ RemoteEnvelope + ActorRefData)
      │      ackAndEnvelope.ToByteString()                            ◀── ALLOC+COPY #W4
      │        → Google.Protobuf sizes, allocs byte[size], then
      │          CodedOutputStream.WriteRawBytes copies W1 bytes
      │          into the new buffer. One full user-payload copy.
      │        → ByteString wrapping the new byte[] (ToByteString uses AttachBytes ✅)
      │
      │  _handle.Write(pdu)   [_handle is AkkaProtocolHandle]
      │    AkkaProtocolHandle.Write(ByteString payload)
      │      WrappedHandle.Write(Codec.ConstructPayload(payload))
      │        AkkaPduProtobuffCodec.ConstructPayload(ByteString payload)
      │          new AkkaProtocolMessage { Payload = payload }.ToByteString()
      │                                                                ◀── ALLOC+COPY #W5
      │            → same protobuf size+alloc+write pattern:
      │              CodedOutputStream.WriteRawBytes copies W4 bytes
      │              into yet another new byte[].
      │            → ByteString wrapping the new byte[]
      │      [WrappedHandle is PipeAssociationHandle]
      │
      │  PipeAssociationHandle.Write(ByteString payload)
      │    Connection.TryEnqueueWrite(payload)
      │      _writeChannel.Writer.TryWrite(payload)  ← zero copy ✅
      │        → ByteString enqueued directly (Channel<ByteString>)
      │
      ▼  [channel consumer — WriteLoopAsync]
 WriteLoopAsync drain loop
      │
      │  WriteFrame(active, payload)  [active: ArrayBufferWriter<byte>]
      │    active.GetSpan(FrameHeaderSize + payload.Length)
      │    BinaryPrimitives.WriteInt32LittleEndian(...)  ← 4-byte header
      │    payload.Span.CopyTo(sp.Slice(FrameHeaderSize))             ◀── COPY #PW8
      │      → copies W5 bytes into the coalesced send buffer.
      │        This is the WRITE-COALESCING cost: intentional.
      │
      ▼
 stream.WriteAsync(active.WrittenMemory, ct)
      │    → single WriteAsync call for the entire batch (may contain
      │       multiple frames from multiple messages)
      ▼
 Kernel send()
```

### 3.2 Write copies — current state table

| ID | Status | Location | Method / Call | Description |
|----|--------|----------|---------------|-------------|
| **#W1** | ❌ **unavoidable (today)** | `MessageSerializer.Serialize` | `serializer.ToBinary(message)` | Serializer returns a freshly-allocated `byte[]`. Cannot be eliminated without a `ToBinary(IBufferWriter<byte>)` API extension on `Serializer`. |
| ~~#W2~~ | ✅ **fixed** | `MessageSerializer.Serialize` | ~~`ByteString.CopyFrom(byte[])`~~ → `UnsafeByteOperations.UnsafeWrap(byte[])` | Was a gratuitous copy of the W1 array. Now wraps without copying. |
| **#W3** | ❌ **remains (small)** | `MessageSerializer.Serialize` | `ByteString.CopyFromUtf8(manifest)` | Manifest string UTF-8 encoded into a fresh `byte[]`. Small (tens of bytes), per-message for non-cached serializers. |
| **#W4** | ❌ **remains** | `AkkaPduProtobuffCodec.ConstructMessage` | `ackAndEnvelope.ToByteString()` | Google.Protobuf allocates `byte[calculatedSize]` and `CodedOutputStream.WriteRawBytes` copies the user payload bytes (from W1 via the attached ByteString) into it. One full user-payload copy. The protobuf POCO allocs (`AckAndEnvelopeContainer`, `RemoteEnvelope`, `ActorRefData`) are additional per-message heap pressure. |
| **#W5** | ❌ **remains** | `AkkaPduProtobuffCodec.ConstructPayload` | `new AkkaProtocolMessage { Payload = payload }.ToByteString()` | The outer protocol envelope: another `byte[calculatedSize]` allocation, another `WriteRawBytes` copy of the W4 bytes. This is the outermost protobuf wrapping layer — one copy per message purely for the protocol framing. |
| ~~#PW6~~ | ✅ **fixed** | `PipeConnection.TryEnqueueWrite` | ~~`payload.ToByteArray()`~~ → `_writeChannel.Writer.TryWrite(payload)` | Was a gratuitous copy to convert `ByteString → byte[]` for the channel. Channel type is now `Channel<ByteString>`; ByteString is passed directly. |
| **#PW8** | ⚠️ **intentional** | `PipeConnection.WriteFrame` | `payload.Span.CopyTo(sp.Slice(FrameHeaderSize))` | Copies W5 bytes into the coalesced `ArrayBufferWriter<byte>` batch. This IS the write-coalescing cost — one `WriteAsync` call per batch amortises the syscall + TLS-record overhead across N messages. The per-message cost is one memcpy; the benefit is N-1 fewer syscalls and N-1 fewer TLS records. Worth keeping. |
| ~~#W7~~ | ✅ **eliminated** | ~~`LengthFieldPrepender`~~ | ~~4-byte `IByteBuffer` per write~~ | DotNetty's `LengthFieldPrepender` allocates a tiny `IByteBuffer` per write. Pipe transport writes the 4-byte LE header inline with `BinaryPrimitives.WriteInt32LittleEndian` — zero alloc. |

**Net write-path user-payload copies:** W4 + W5 + PW8 = **3 full copies** of the user payload between the serializer output and the kernel write buffer.

> **CopilotNotes:** The three write copies map to the three nesting layers:
> 1. **W4 (`ConstructMessage`)**: wraps user payload in `AckAndEnvelopeContainer` (seq/ack envelope)
> 2. **W5 (`ConstructPayload`)**: wraps the above in `AkkaProtocolMessage` (protocol envelope)
> 3. **PW8 (`WriteFrame`)**: copies the above into the coalesced send batch
>
> W4 and W5 are the direct consequence of having **two layers of protobuf 
> serialisation** for every outbound message. Each layer calls
> `ToByteString()` which must produce a complete contiguous `byte[]` to
> hand to the next layer's `CodedOutputStream`. 🌸

---

## 4. Where the Copies Are vs. Where They Could Be Avoided

| # | Copy / Alloc | User-payload sized? | Eliminable? | What prevents it today |
|---|---|---|---|---|
| PR1 | `ByteString.CopyFrom(frame.FirstSpan)` — recv → ByteString | ✅ Yes (PDU = user payload + envelope) | Partially — allocation can become a pool-rent; **copy is required** to cross the `PipeReader` segment lifecycle boundary | `ByteString` has no `Dispose`; it cannot alias memory the `PipeReader` will recycle on `AdvanceTo`. A custom `IInboundPayload : IDisposable` holding an `IMemoryOwner<byte>` would eliminate the allocation while keeping one memcpy. |
| R2 | `AkkaProtocolMessage.Parser.ParseFrom` inner `bytes Payload` | ✅ Yes | Yes — hand-roll the outer envelope reader to avoid `ParseFrom` for the payload field; or upgrade Google.Protobuf ≥ 3.27 for `AttachBytes` semantics | `Parser.ParseFrom` eagerly copies every `bytes` field. The outer envelope is tiny; we could read `tag + varint(len)` by hand and slice without copying. |
| R3 | `AckAndEnvelopeContainer.Parser.ParseFrom` inner `bytes Message` | ✅ Yes (this IS the user payload) | Yes — same fix as R2 applied to the inner envelope | Same root cause: protobuf `ReadBytes()` always copies. Inner `bytes Message` is the user-serialized payload — this is the biggest copy on the read side. |
| R4-fallback | `Serializer.FromBinary(ReadOnlyMemory<byte>, ...)` default `bytes.ToArray()` | ✅ Yes | Partially — infrastructure exists; serializers must opt-in | The `ReadOnlyMemory<byte>` overload exists but defaults to `bytes.ToArray()`. Serializers must override it to eliminate the copy. Core serializers (`NewtonSoftJson`, `Hyperion`, protobuf-based) do not yet do so. |
| R5 | `ToStringUtf8()` — manifest string | No (small) | Partially (manifest interning) | `ByteString.ToStringUtf8()` always allocates. A `ConcurrentDictionary<ByteString, string>` intern cache per serializer-id would eliminate per-message manifest string allocs in steady state. |
| W1 | `serializer.ToBinary(message)` | ✅ Yes | No (today) | `Serializer.ToBinary` returns `byte[]` by contract; changing to `ToBinary(IBufferWriter<byte>)` is a significant public API addition. |
| W3 | `ByteString.CopyFromUtf8(manifest)` | No (small) | Partially (cache) | Cache `(SerializerId, manifest) → ByteString`; in steady state the same manifest appears millions of times. |
| W4 | `ackAndEnvelope.ToByteString()` | ✅ Yes | Yes — write directly into pooled `IBufferWriter<byte>` | `ToByteString()` forces a `byte[]` allocation. A hand-rolled or `IBufferWriter<byte>`-based codec would write directly into a pooled output buffer. |
| W5 | `new AkkaProtocolMessage{}.ToByteString()` | ✅ Yes | Yes — hand-rolled tag+varint+copy OR merged with W4 | Another `byte[]` allocation per message for a wrapper that adds ~4–6 bytes. Could be merged with W4 into a single pooled write, or the outer protobuf wrapping could be replaced with a lightweight custom frame tag. |
| PW8 | `payload.Span.CopyTo(...)` write coalescing | ✅ Yes | No — this is the write-coalescing cost (intentional) | Removes N-1 syscalls and N-1 TLS records. The memcpy cost (~5–10 ns/KiB) is far cheaper than those savings. |

---

## 5. Double Protobuf Nesting — The Root Cause

Both the excessive read copies (R2 + R3) and write copies (W4 + W5) are
caused by the same design decision: **every user message is wrapped in two
concentric protobuf messages before hitting the wire**:

```
Wire frame (length-prefixed):
└─ AkkaProtocolMessage  (outer envelope — one of: Payload | Instruction)
   └─ bytes Payload
      └─ AckAndEnvelopeContainer  (inner envelope — seq/ack + actor addresses)
         └─ RemoteEnvelope
            └─ SerializedMessage  (Payload proto type — alias for what's in AkkaPduCodec.cs)
               └─ bytes Message   ← actual user-serialized payload
```

On the **write side**, `ToByteString()` is called at each nesting level:

1. `AckAndEnvelopeContainer.ToByteString()` → `byte[]` containing the
   user payload inside a protobuf frame. (`ALLOC+COPY #W4`)
2. `AkkaProtocolMessage { Payload = ... }.ToByteString()` → `byte[]`
   wrapping the W4 bytes inside a second protobuf frame. (`ALLOC+COPY #W5`)

On the **read side**, `Parser.ParseFrom(ByteString)` is called at each level:

1. `AkkaProtocolMessage.Parser.ParseFrom(raw)` — reads the outer envelope,
   and protobuf's `CodedInputStream.ReadBytes()` copies the inner `bytes Payload`
   field into a fresh `byte[]`. (`COPY #R2`)
2. `AckAndEnvelopeContainer.Parser.ParseFrom(inner)` — reads the inner
   envelope, and `ReadBytes()` copies the `bytes Message` field (the user
   payload) into yet another fresh `byte[]`. (`COPY #R3`)

Because Google.Protobuf's `ReadBytes()` always copies (it must produce an
immutable `ByteString`), and because `ToByteString()` always allocates a
fresh contiguous `byte[]`, the two-level nesting **guarantees at least two
full user-payload copies on each path**.

> **CopilotNotes:** The Phase 2 design documented in
> `akka-remote-akka-protocol-redux.md` proposes replacing the outer
> `AkkaProtocolMessage` envelope with a single-byte tag prefix (in the
> `inline-protocol = on` mode). This would collapse W4+W5 into one
> serialize step and R2+R3 into one parse step — halving the
> protobuf-layer copies on both sides. The inner `AckAndEnvelopeContainer`
> layer would still be present for seq/ack but could be written into a
> pooled `IBufferWriter<byte>` to avoid W4's allocation. 🌸

---

## 6. Already-fixed Copies (for completeness)

These copies existed in earlier code or in the DotNetty transport and have
already been addressed in the current pipe transport implementation:

| ID | Fixed by | Before | After |
|----|----------|--------|-------|
| **#R0** (recv alloc) | `PipeReader.Create(stream, ...)` with pooled options | `UnpooledByteBufferAllocator` in DotNetty → new `byte[]` per recv | PipeReader rents from `MemoryPool<byte>.Shared` — zero per-recv allocation |
| **#R4** (deserialization call) | `MessageSerializer.Deserialize` | `messageProtocol.Message.ToByteArray()` | `messageProtocol.Message.Memory` (passes `ReadOnlyMemory<byte>` to the `Serialization` overload) |
| **#W2** (serializer output wrap) | `MessageSerializer.Serialize` | `ByteString.CopyFrom(serializer.ToBinary(message))` | `UnsafeByteOperations.UnsafeWrap(serializer.ToBinary(message))` |
| **#PW6** (channel enqueue) | `PipeConnection.TryEnqueueWrite` + `Channel<ByteString>` | `_writeChannel.Writer.TryWrite(payload.ToByteArray())` with `Channel<byte[]>` | `_writeChannel.Writer.TryWrite(payload)` — ByteString passed directly |
| **#PR1b (slow path double)** | `PipeConnection.ReadLoopAsync` | `ByteString.CopyFrom(frame.ToArray())` — 2 allocs + 2 copies | `UnsafeByteOperations.UnsafeWrap(frame.ToArray())` — 1 alloc + 1 copy |
| **#W7** (LengthField alloc) | Inline framing in `WriteFrame` | DotNetty `LengthFieldPrepender` allocates a 4-byte `IByteBuffer` per write | `BinaryPrimitives.WriteInt32LittleEndian` into the active `ArrayBufferWriter<byte>` — zero alloc |

---

## 7. Allocation Hotspot Cheat-Sheet (Combined Read + Write)

| ID | Status | File + Method | Approx size | Notes |
|----|--------|---------------|-------------|-------|
| PR1 | ❌ remains | `PipeConnection.ReadLoopAsync` → `ByteString.CopyFrom(frame.FirstSpan)` | Full PDU | Crosses PipeReader refcount domain. Cannot zero-copy without `IInboundPayload` lifecycle change. |
| R2 | ❌ remains | `AkkaPduProtobuffCodec.DecodePdu` → `AkkaProtocolMessage.Parser.ParseFrom(raw)` | Full PDU payload | Outer protobuf envelope parse; `ReadBytes()` copies inner `bytes Payload`. |
| R3 | ❌ remains | `AkkaPduProtobuffCodec.DecodeMessage` → `AckAndEnvelopeContainer.Parser.ParseFrom(raw)` | Full user payload | Inner protobuf envelope parse; `ReadBytes()` copies inner `bytes Message`. |
| R4-fallback | ⚠️ per-serializer | `Serializer.FromBinary(ReadOnlyMemory<byte>, ...)` (base default) | Full user payload | `bytes.ToArray()` in default impl. Eliminated when serializer overrides the `ReadOnlyMemory` path. |
| R5 | ❌ remains (small) | `MessageSerializer.Deserialize` → `ToStringUtf8()` | Manifest (~bytes) | Per-message string alloc for manifest. |
| W1 | ❌ unavoidable (today) | `MessageSerializer.Serialize` → `serializer.ToBinary(message)` | Full user payload | Serializer API returns `byte[]`. |
| W3 | ❌ remains (small) | `MessageSerializer.Serialize` → `ByteString.CopyFromUtf8(manifest)` | Manifest (~bytes) | Per-message UTF-8 encode. |
| W4 | ❌ remains | `AkkaPduProtobuffCodec.ConstructMessage` → `ackAndEnvelope.ToByteString()` | Full PDU | Inner protobuf `byte[]` alloc + `WriteRawBytes` user-payload copy. |
| W5 | ❌ remains | `AkkaPduProtobuffCodec.ConstructPayload` → `AkkaProtocolMessage{}.ToByteString()` | Full PDU | Outer protobuf `byte[]` alloc + `WriteRawBytes` copy of W4 bytes. |
| PW8 | ⚠️ intentional | `PipeConnection.WriteFrame` → `payload.Span.CopyTo(...)` | Full PDU | Write-coalescing: one syscall per batch vs. one per message. Keep. |

---

## 8. Observations for Redesign

> **Note:** This section records observations only — no new design is
> proposed here. See the companion docs for concrete Phase 2 proposals.

1. **Two protobuf wrapping layers are the primary cost.** W4+W5 on the write
   side and R2+R3 on the read side each represent one of the two nesting
   levels. A redesign that eliminates one level (e.g., replacing the outer
   `AkkaProtocolMessage` with a lightweight custom tag byte) would halve
   these costs immediately.

2. **The `ByteString` API as a layer-boundary type blocks zero-copy.**
   `ByteString` is immutable and has no lifecycle management, so every layer
   that hands off a `ByteString` must have already copied into a
   freshly-allocated `byte[]`. Replacing the inter-layer contract with a
   `ReadOnlyMemory<byte>` + explicit lifetime (or `IMemoryOwner<byte>`) would
   allow the upper layers to read directly from the PipeReader's pooled
   buffer segment.

3. **PR1 (recv → ByteString) is the unavoidable boundary today.** The
   `IHandleEventListener.Notify(IHandleEvent)` SPI takes `InboundPayload(ByteString)`.
   The PipeReader segment lives only until `AdvanceTo` is called, but the
   actor message can be observed arbitrarily late. A redesign of
   `IHandleEventListener` to accept `ReadOnlyMemory<byte>` (or a pooled
   `IInboundPayload` with an explicit `Dispose`) would allow renting rather
   than allocating for this copy.

4. **Serializer `FromBinary(ReadOnlyMemory<byte>)` infrastructure exists but
   is not yet leveraged.** The chain `MessageSerializer → Serialization →
   Serializer` now correctly threads `ReadOnlyMemory<byte>` all the way to
   the serializer virtual, but the default implementation calls
   `bytes.ToArray()`. For the zero-copy read path to be realised end-to-end,
   individual serializers (`NewtonSoftJsonSerializer`, `HyperionSerializer`,
   the protobuf-based ones) must override `FromBinary(ReadOnlyMemory<byte>, ...)`.

5. **Write coalescing (`PW8`) is a net win.** The memcpy cost per message is
   trivially small compared to the syscall + TLS-record-per-message cost it
   avoids. Any redesign should preserve the double-buffer ping-pong batch
   write.

6. **The `ProtocolStateActor` mailbox adds two actor-message hops per frame**
   (PipeConnection → ProtocolStateActor → EndpointReader). Each hop
   allocates an actor envelope + mailbox node. The Phase 2 inline-FSM design
   would eliminate both hops for the hot data path (Payload frames), reducing
   this to a direct method call on the read loop. Control frames (heartbeat,
   associate, disassociate) are rare and tolerate the hop cost.

7. **`Channel<ByteString>` write-side enqueue is already zero-copy** (PW6
   fixed). The next bottleneck on the write side is `W4+W5` in the codec,
   not the channel or the I/O loop.

8. **`ActorRefData` and envelope POCO allocations** (`AckAndEnvelopeContainer`,
   `RemoteEnvelope`, `ActorRefData` × 1-2 per send) are per-message but
   small. A per-`EndpointWriter` LRU cache keyed on `IActorRef → ActorRefData`
   (or pre-serialised partial protobuf bytes for the recipient field) would
   cut both string formatting and POCO allocation in steady-state, especially
   for high-fanout actors.

---

*Document generated May 2026. Reflects `dev` branch at the time of writing.
Cross-reference: `akka-remote-read-pipeline-allocations.md` §7,
`akka-remote-write-pipeline-allocations.md` §7,
`akka-remote-akka-protocol-redux.md`. UwU~ 🌸*

