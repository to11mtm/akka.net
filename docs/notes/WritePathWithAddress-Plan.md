# `ActorPath.WritePathWithAddress` — Analysis & Implementation Plan

> Status: design proposal · target API surface:
> `public void WritePathWithAddress(IBufferWriter<char> bufferWriter, Address address, bool includeUid)`
>
> Goal: produce the **exact same character sequence** as
> `ToStringWithAddress(Address address, bool includeUid)` but stream it directly
> into a caller-provided `IBufferWriter<char>` with **zero intermediate `string`
> allocations** on the hot path. uwu ✨

---

## 1. What does `ToStringWithAddress(Address, bool)` actually produce?

Reading `ActorPath.ToStringWithAddress(Address, bool)` (the private overload at
the bottom of the file) and following `Join(ReadOnlySpan<char>, long?)`, the
output for a depth-`N` path is:

```
<addressString>/<name_1>/<name_2>/.../<name_N>[#<uid>]
```

Where:

1. `<addressString>` is the **effective address** stringified by
   `Address.ToString()` / `CreateLazyToString`:
   - `protocol://system@host:port`     (host + port set)
   - `protocol://system@host`          (host set, no port — rare)
   - `protocol://system`               (no host)
2. The effective address is `this.Address` when this path already has both
   `Host` and `Port`; otherwise the `address` argument is used.
3. `<name_i>` are the path segments walked from `Root → leaf` (parent chain is
   stored bottom-up so it currently materialises in reverse).
4. The trailing `#<uid>` is written only when `includeUid == true` **and**
   `Uid != ActorCell.UndefinedUid`.
5. **Special case**: if `IgnoreActorRef.IsIgnoreRefPath(this)` is true, we
   delegate to `ToString()` which uses the path's own address — never the
   argument. The new writer method must honour this too.

So the algorithm is:

```
write effective-address chars
for each name from root → leaf:
    write '/'
    write name
if includeUid && Uid != UndefinedUid:
    write '#'
    write uid as base-10 digits
```

---

## 2. What does the *current* implementation cost?

`ToStringWithAddress → ToStringWithAddress(address, false/true) → Join(prefix, uid?)`:

| Step                                              | Allocation                                                                |
| ------------------------------------------------- | ------------------------------------------------------------------------- |
| `effectiveAddress.ToString()`                     | 1 × `string` (lazily cached on `Address._toString` — amortised free)      |
| `Join` builds total length, allocates `char[]`    | **stackalloc when `<1024` chars**, otherwise 1 × `char[]`                 |
| `buffer.ToString()` returned to the caller        | **1 × `string` (the return value — unavoidable for a `string` API)**      |
| Parent-chain walk done **twice** (length + write) | none, but two traversals of a linked list                                 |

The two unavoidable forces of the current API are: (a) it must return a managed
`string`, and (b) `Join` is built around the assumption that the prefix is
already a contiguous `ReadOnlySpan<char>`, which forces materialising the
address as a string up front.

The proposed `WritePathWithAddress` removes **both** of those forces — we no
longer need a final `string`, and the address can be **streamed** segment-by-
segment straight into the writer.

---

## 3. Constraints & invariants for the new API

1. **Identical bytes**: `WritePathWithAddress(w, addr, inc); w.As<string>() ==
   path.ToStringWithAddress(addr, inc)` for every input. (We'll add an xUnit
   property test asserting this with a small `ArrayBufferWriter<char>`.)
2. **Zero allocations on the steady-state path**, excluding what the caller's
   `IBufferWriter<char>` itself may rent.
3. **Single forward traversal of the parent chain**, since we no longer need
   the `string` return type and can pre-size with a small `Span<>` scratch.
4. **No reliance on `Address.ToString()`** — write protocol / host / port
   directly to the writer.
5. **Must remain safe** for `depth > ~64`, which is the natural ceiling for
   stack-based scratch buffers.

---

## 4. Proposed implementation

### 4.1 Helper: `WriteAddress(IBufferWriter<char>, Address)`

Mirrors `Address.CreateLazyToString` exactly, but streams via `GetSpan` /
`Advance`. Conceptually:

```csharp
internal static void WriteAddressTo(this Address addr, IBufferWriter<char> w)
{
    // protocol "://" system
    WriteString(w, addr.Protocol);
    WriteSpan(w, "://".AsSpan());
    WriteString(w, addr.System);

    if (!string.IsNullOrEmpty(addr.Host))
    {
        WriteSpan(w, "@".AsSpan());
        WriteString(w, addr.Host);
        if (addr.Port.HasValue)
        {
            WriteSpan(w, ":".AsSpan());
            WriteInt32(w, addr.Port.Value); // uses SpanHacks.TryFormat
        }
    }
}
```

Where `WriteString` / `WriteSpan` are tiny helpers that do
`var s = w.GetSpan(src.Length); src.CopyTo(s); w.Advance(src.Length);`.

`WriteInt32` uses `SpanHacks.PositiveInt64SizeInCharacters` + `TryFormat` (we
already use it elsewhere in `Join`) to write the port digits straight into the
buffer with no intermediate string.

> CopilotNotes: This method will live as a `static internal` helper on
> `Address` so it can access `Protocol/System/Host/Port` without re-validating
> them, and so other Akka.Remote codecs can reuse it (e.g. the future
> MessagePack `SerializeActorRef` zero-copy path~).

### 4.2 Helper: walk parent chain into a stack-allocated scratch

We need to write names from `Root → leaf`, but the linked list is leaf → root.

For the **common case** where `_depth <= 64`, allocate a `Span<string>`
on the stack to hold the names in root-order:

```csharp
const int StackChainLimit = 64;
Span<string> nameStack = _depth <= StackChainLimit
    ? stackalloc string[StackChainLimit]   // NOTE: see §4.4 for actual mechanism
    : new string[_depth];

// Fill in reverse so index 0 == root child name.
var p = this;
for (var i = _depth - 1; i >= 0; i--)
{
    nameStack[i] = p!.Name;
    p = p.Parent;
}
```

We then iterate `0..(_depth-1)` and write `'/'` + `nameStack[i]` to the
writer in true forward order. Single traversal of the parent chain — same as
`Join` does today, but without building a reverse-fill buffer.

### 4.3 Putting it together

```csharp
public void WritePathWithAddress(IBufferWriter<char> bufferWriter, Address address, bool includeUid)
{
    if (bufferWriter is null) throw new ArgumentNullException(nameof(bufferWriter));

    // 1) IgnoreActorRef short-circuit — must match ToStringWithAddress / ToString.
    //    These paths *ignore* the supplied `address` and emit their own.
    var effective = IgnoreActorRef.IsIgnoreRefPath(this)
        ? Address
        : (Address is { Host: not null, Port: not null } ? Address : address);

    // 2) Stream the address.
    effective.WriteTo(bufferWriter);

    // 3) Stream the path segments (root → leaf).
    if (_depth == 0)
    {
        // Root path renders as ".../" — matches Join's depth-0 branch.
        WriteChar(bufferWriter, '/');
        return;
    }

    // Scratch for forward-ordered names. _depth is bounded by hierarchy depth,
    // which is *practically* tiny; we fall back to the heap above the limit.
    const int StackChainLimit = 64;
    string[]? rented = null;
    Span<string?> chain = _depth <= StackChainLimit
        ? stackalloc string?[StackChainLimit].Slice(0, _depth) // see §4.4
        : (rented = ArrayPool<string>.Shared.Rent(_depth)).AsSpan(0, _depth);

    try
    {
        var p = this;
        for (var i = _depth - 1; i >= 0; i--)
        {
            chain[i] = p!.Name;
            p = p.Parent;
        }

        for (var i = 0; i < _depth; i++)
        {
            WriteChar(bufferWriter, '/');
            WriteString(bufferWriter, chain[i]!);
        }

        // 4) Optional UID fragment, written using SpanHacks (no string alloc).
        if (includeUid && Uid != ActorCell.UndefinedUid)
        {
            WriteChar(bufferWriter, '#');
            WriteInt64(bufferWriter, Uid);
        }
    }
    finally
    {
        if (rented is not null)
            ArrayPool<string>.Shared.Return(rented, clearArray: true);
    }
}
```

Allocation profile:
- `_depth <= 64`: **zero managed allocations** beyond what the writer rents.
- `_depth >  64`: one pooled `string[]`, returned immediately. Still zero
  per-character allocations.
- The `string` *references* in the scratch span are existing interned-ish
  `Name` values held on `ActorPath` nodes — they are not copied, only their
  references are placed in the stack span.

### 4.4 The `stackalloc string?[]` caveat

`stackalloc` is restricted to *unmanaged* types, so we cannot literally
`stackalloc string?[N]`. The implementation will use one of:

1. **`Span<int>` of name lengths + walk twice**: stackalloc length offsets,
   compute total length, allocate one writer span, then second walk to copy
   chars in. This is what `Join` does today and is fully GC-free.
2. **`Span<IntPtr>` via `Unsafe.As<string, IntPtr>`**: store the reference as
   an `IntPtr` and round-trip it. Pragmatic but spooky — needs `[SkipLocalsInit]`
   and careful safe-stack handling, not recommended.
3. **`InlineArray<64, string?>`** (.NET 8+): clean but multi-TFM ifdef hell
   because we still target `net48` / `netstandard2.0`.

**Recommendation: option 1** — it perfectly mirrors `Join`'s approach, keeps
all current target frameworks happy, and is genuinely allocation-free:

```csharp
// First pass: collect Name.Length per node into a stackalloc Span<int> +
// remember total. Second pass: walk parents again and write each segment.
Span<int> lengths = _depth <= 64 ? stackalloc int[64] : new int[_depth];
```

The "two-pass" cost is two pointer-chases per node, which for actor depths
(typically `< 8`) is negligible compared to even one heap allocation.

### 4.5 Final shape (recommended)

```csharp
public void WritePathWithAddress(IBufferWriter<char> bufferWriter, Address address, bool includeUid)
{
    if (bufferWriter is null) throw new ArgumentNullException(nameof(bufferWriter));

    var effective = IgnoreActorRef.IsIgnoreRefPath(this)
        ? Address
        : (Address is { Host: not null, Port: not null } ? Address : address);

    WriteAddress(bufferWriter, effective);

    if (_depth == 0)
    {
        Write(bufferWriter, '/');
        return;
    }

    // Stackalloc-friendly: capture name lengths during a single back-walk.
    const int InlineCap = 64;
    Span<int> lengths = _depth <= InlineCap
        ? stackalloc int[InlineCap].Slice(0, _depth)
        : new int[_depth];

    var p = this;
    for (var i = _depth - 1; i >= 0; i--)
    {
        lengths[i] = p!.Name.Length;
        p = p.Parent;
    }

    // Second walk: write '/' + name for each level, root → leaf.
    // We re-walk the parent chain instead of buffering string refs to
    // remain entirely `unmanaged`-safe inside stackalloc.
    WriteSegments(bufferWriter, this, _depth, lengths);

    if (includeUid && Uid != ActorCell.UndefinedUid)
    {
        Write(bufferWriter, '#');
        WriteInt64(bufferWriter, Uid);
    }
}

private static void WriteSegments(IBufferWriter<char> w, ActorPath leaf, int depth, ReadOnlySpan<int> lengths)
{
    // Compute the total length of the segment block ( '/' + name per node ).
    var total = depth; // one '/' per segment
    for (var i = 0; i < depth; i++) total += lengths[i];

    var dest = w.GetSpan(total);
    // Walk leaf → root, writing segments at the *end* of `dest` and moving
    // back. This avoids needing a forward-ordered name list.
    var pos = total;
    var p = leaf;
    while (p!._depth > 0)
    {
        var name = p.Name.AsSpan();
        pos -= name.Length;
        name.CopyTo(dest.Slice(pos));
        pos -= 1;
        dest[pos] = '/';
        p = p.Parent;
    }
    w.Advance(total);
}
```

This variant uses `GetSpan(total)` once for the whole segment block and writes
it in reverse fill — identical to what `Join` does internally — but every
character lands directly into the writer's buffer. The `lengths` span is only
used to compute `total` upfront so we can ask `GetSpan` for the right size.

---

## 5. Tests to add

Create `WritePathWithAddressSpec.cs` under `src/core/Akka.Tests/Actor/` that
exercises **byte-for-byte parity** with `ToStringWithAddress` across:

| Scenario                                          | Path                                              |
| ------------------------------------------------- | ------------------------------------------------- |
| Root-only local path                              | `new RootActorPath(new Address("akka","sys"))`    |
| Single child, local address                       | `root / "user"`                                   |
| Deep child with remote address (path-owned)       | `root("akka.tcp","sys","127.0.0.1",1234) / a/b/c` |
| Deep child, includeUid=true (uid != Undefined)    | as above with `.WithUid(42L)`                     |
| Path with local addr + remote `address` arg       | `localRoot/a/b` with arg `akka.tcp@host:1`        |
| `IgnoreActorRef` path (asserts arg is ignored)    | the canonical ignore-ref path                     |
| Very deep path (`_depth = 128`) — pool path test  | 128 nested children                               |

Each test:

```csharp
var expected = path.ToStringWithAddress(addr, includeUid);
var writer   = new ArrayBufferWriter<char>(expected.Length);
path.WritePathWithAddress(writer, addr, includeUid);
new string(writer.WrittenSpan).Should().Be(expected);
```

Plus a **FsCheck/property-based** generator that builds random paths and
asserts the parity invariant.

---

## 6. Follow-ups (out of scope here but enabled by this work)

1. `Address.WriteTo(IBufferWriter<char>)` becomes the reusable building block
   for **any** code path that currently does `addr.ToString()` then writes
   chars — including the MessagePack codec's `SerializeActorRef`.
2. `ActorPath.ToSerializationFormatWithAddress(Address)` can be rewritten to
   call `WritePathWithAddress` against a pooled `ArrayBufferWriter<char>` to
   get a single `string` allocation (the final `new string(span)`) with no
   intermediate `Address.ToString()` allocation.
3. An `IBufferWriter<byte>` UTF-8 sibling (`WriteUtf8PathWithAddress`) becomes
   trivially derivable for protobuf / msgpack write paths that target byte
   buffers directly, avoiding the UTF-16 → UTF-8 transcode step entirely.
   *(That's the holy-grail allocation-free actor-ref serialisation~ 💖)*

---

*Ami-chan's TL;DR*: walk the parent chain once to sum the segment length,
ask the writer for one span big enough for `address + segments + uid`, fill it
end-to-start for the segments and forward for the address & uid. Zero `string`s,
zero `char[]`s, much fast, very wow. uwu 🌸

