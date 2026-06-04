//-----------------------------------------------------------------------
// <copyright file="IPooledInboundPayload.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable
using System;
using System.Buffers;

namespace Akka.Remote.Transport
{
    /// <summary>
    /// INTERNAL API.
    ///
    /// Represents a single inbound frame's payload managed with explicit ownership and lifetime,
    /// allowing zero-copy or pool-rented delivery through the receive pipeline. ✨
    ///
    /// <para>
    /// Two concrete implementations exist:
    /// <list type="bullet">
    ///   <item>
    ///     <see cref="RentedInboundPayload"/> — rented from <see cref="MemoryPool{T}.Shared"/>;
    ///     safe to pass across actor mailbox boundaries. Caller <b>must</b> call <see cref="IDisposable.Dispose"/>
    ///     when the bytes are no longer needed so the buffer is returned to the pool.
    ///   </item>
    ///   <item>
    ///     <see cref="SegmentAliasPayload"/> — zero-copy alias over a live <see cref="System.IO.Pipelines.PipeReader"/>
    ///     segment. Valid only within the same read-loop iteration (before <c>AdvanceTo</c>).
    ///     <see cref="IDisposable.Dispose"/> is a no-op.
    ///   </item>
    /// </list>
    /// </para>
    ///
    /// <!-- CopilotNotes: The key invariant is simple — whoever creates an IPooledInboundPayload
    ///      is responsible for eventually disposing it. The lazy InboundPayload.Payload getter
    ///      disposes the pooled payload once it has materialised a ByteString copy, so legacy
    ///      callers that just access Payload get correct lifetime management for free. 🌸 -->
    /// </summary>
    internal interface IPooledInboundPayload : IDisposable
    {
        /// <summary>A read-only view of the frame payload bytes.</summary>
        ReadOnlyMemory<byte> Memory { get; }

        /// <summary>A span over the frame payload bytes.</summary>
        ReadOnlySpan<byte> Span { get; }

        /// <summary>Number of valid bytes in <see cref="Memory"/>.</summary>
        int Length { get; }
    }

    /// <summary>
    /// INTERNAL API.
    ///
    /// A pool-backed inbound payload that owns memory rented from
    /// <see cref="MemoryPool{T}.Shared"/>. 🌸
    ///
    /// <para>
    /// Safe to send across actor mailbox boundaries because the memory is owned
    /// (not an alias of a live <see cref="System.IO.Pipelines.PipeReader"/> segment).
    /// The recipient actor <b>must</b> dispose this when done to return the buffer to the pool.
    /// </para>
    ///
    /// <para>
    /// Prefer the <see cref="Rent(ReadOnlySequence{byte})"/> factory overload when
    /// consuming a multi-segment <c>ReadOnlySequence&lt;byte&gt;</c>; it always does
    /// a single <c>CopyTo</c> into the rented buffer.
    /// </para>
    ///
    /// <!-- CopilotNotes: MemoryPool<byte>.Shared returns at least the requested byte count but
    ///      may round up. We track the exact frame length in _length so callers always see
    ///      correctly-bounded slices. Thread safety: created by the read-loop thread and then
    ///      owned by exactly one actor (sequential message processing). No concurrent access. 🌸 -->
    /// </summary>
    internal sealed class RentedInboundPayload : IPooledInboundPayload
    {
        private IMemoryOwner<byte>? _owner;
        private readonly int _length;

        private RentedInboundPayload(IMemoryOwner<byte> owner, int length)
        {
            _owner = owner;
            _length = length;
        }

        /// <summary>
        /// Rents a buffer large enough to hold <paramref name="source"/> and copies into it.
        /// </summary>
        /// <param name="source">The source sequence to copy (may be multi-segment).</param>
        public static RentedInboundPayload Rent(ReadOnlySequence<byte> source)
        {
            var length = (int)source.Length;
            var owner  = MemoryPool<byte>.Shared.Rent(length);
            source.CopyTo(owner.Memory.Span);
            return new RentedInboundPayload(owner, length);
        }

        /// <summary>
        /// Rents a buffer large enough to hold <paramref name="source"/> and copies into it.
        /// </summary>
        /// <param name="source">The source span to copy.</param>
        public static RentedInboundPayload Rent(ReadOnlySpan<byte> source)
        {
            var owner = MemoryPool<byte>.Shared.Rent(source.Length);
            source.CopyTo(owner.Memory.Span);
            return new RentedInboundPayload(owner, source.Length);
        }

        /// <inheritdoc/>
        public ReadOnlyMemory<byte> Memory => _owner!.Memory.Slice(0, _length);

        /// <inheritdoc/>
        public ReadOnlySpan<byte> Span => _owner!.Memory.Span.Slice(0, _length);

        /// <inheritdoc/>
        public int Length => _length;

        /// <summary>Returns the rented buffer to <see cref="MemoryPool{T}.Shared"/>. Safe to call multiple times. 🌸</summary>
        public void Dispose()
        {
            _owner?.Dispose();
            _owner = null;
        }
    }

    /// <summary>
    /// INTERNAL API.
    ///
    /// A zero-copy alias over a <see cref="System.IO.Pipelines.PipeReader"/> segment for use
    /// <b>within a single read-loop iteration only</b>. ✨
    ///
    /// <para>
    /// The underlying memory is owned by the <c>PipeReader</c> and is valid only until
    /// <c>PipeReader.AdvanceTo</c> is called. This type must <b>never</b> be sent across
    /// actor mailbox boundaries — use <see cref="RentedInboundPayload"/> instead.
    /// </para>
    ///
    /// <para>
    /// <see cref="Dispose"/> is a no-op because lifetime is managed by the <c>PipeReader</c>.
    /// </para>
    ///
    /// <!-- CopilotNotes: SegmentAliasPayload is infrastructure for PR-C's inline protocol where
    ///      frames are processed synchronously inside ReadLoopAsync before any AdvanceTo.
    ///      PR-B's PipeConnection always promotes to RentedInboundPayload before any mailbox hop. 🌸 -->
    /// </summary>
    internal sealed class SegmentAliasPayload : IPooledInboundPayload
    {
        private readonly ReadOnlyMemory<byte> _memory;

        /// <summary>Creates an alias over <paramref name="memory"/> without copying.</summary>
        public SegmentAliasPayload(ReadOnlyMemory<byte> memory) => _memory = memory;

        /// <inheritdoc/>
        public ReadOnlyMemory<byte> Memory => _memory;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> Span => _memory.Span;

        /// <inheritdoc/>
        public int Length => _memory.Length;

        /// <inheritdoc/>
        /// <remarks>No-op — lifetime is managed by the owning <c>PipeReader</c>.</remarks>
        public void Dispose() { /* no-op 🌸 */ }
    }
}

