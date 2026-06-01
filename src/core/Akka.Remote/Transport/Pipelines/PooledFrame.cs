//-----------------------------------------------------------------------
// <copyright file="PooledFrame.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable
using System;
using System.Buffers;
using Google.Protobuf;

namespace Akka.Remote.Transport.Pipelines
{
    /// <summary>
    /// INTERNAL API.
    ///
    /// Represents a single outbound Akka-protocol frame ready to be written
    /// into the coalesced send batch. 🌸
    ///
    /// <para>
    /// Implementations must be disposed by the write loop after the frame
    /// bytes have been copied into the <c>ArrayBufferWriter&lt;byte&gt;</c> batch.
    /// </para>
    ///
    /// <!-- CopilotNotes: Two implementations:
    ///      - PooledFrame   — rented from MemoryPool<byte>.Shared; eliminates GC pressure.
    ///      - ByteStringPooledFrame — wraps a legacy ByteString; Dispose is no-op; used
    ///        on the legacy (zero-copy-codec = off) path so PipeConnection.WriteLoopAsync
    ///        can use a single Channel<IPooledFrame> for both paths. -->
    /// </summary>
    internal interface IPooledFrame : IDisposable
    {
        /// <summary>A read-only view of the frame bytes to be copied into the send batch.</summary>
        ReadOnlySpan<byte> WrittenSpan { get; }

        /// <summary>The number of valid bytes in <see cref="WrittenSpan"/>.</summary>
        int Length { get; }
    }

    /// <summary>
    /// INTERNAL API.
    ///
    /// A pool-backed, disposable outbound frame that implements both
    /// <see cref="IPooledFrame"/> and <see cref="IBufferWriter{T}"/> so the
    /// hand-written codec can write directly into it. ✨
    ///
    /// <para>
    /// Usage:
    /// <code>
    /// var frame = PooledFrame.Rent(neededCapacity);
    /// OuterEnvelopeWriter.WritePayloadFrame(frame, innerBytes);
    /// // frame is now ready — enqueue onto Channel&lt;IPooledFrame&gt;.
    /// // WriteLoopAsync disposes the frame after copying into the batch.
    /// </code>
    /// </para>
    ///
    /// <!-- CopilotNotes: MemoryPool<byte>.Shared.Rent(minCapacity) returns at least
    ///      minCapacity bytes; in practice it may return more (pool rounds up). We
    ///      track how many bytes have actually been written via _written. Thread safety:
    ///      a frame is written by exactly one producer thread, then consumed (read + disposed)
    ///      by exactly one consumer thread (the WriteLoopAsync). The Channel provides
    ///      the required happens-before barrier between them. 🌸 -->
    /// </summary>
    internal sealed class PooledFrame : IPooledFrame, IBufferWriter<byte>
    {
        private IMemoryOwner<byte>? _owner;
        private int _written;

        private PooledFrame(IMemoryOwner<byte> owner) => _owner = owner;

        /// <summary>
        /// Rents a <see cref="PooledFrame"/> backed by at least
        /// <paramref name="minimumCapacity"/> bytes from <see cref="MemoryPool{T}.Shared"/>.
        /// </summary>
        public static PooledFrame Rent(int minimumCapacity) =>
            new(MemoryPool<byte>.Shared.Rent(minimumCapacity));

        // ── IBufferWriter<byte> ──────────────────────────────────────────────

        /// <inheritdoc/>
        public void Advance(int count) => _written += count;

        /// <inheritdoc/>
        public Memory<byte> GetMemory(int sizeHint = 0) =>
            _owner!.Memory.Slice(_written);

        /// <inheritdoc/>
        public Span<byte> GetSpan(int sizeHint = 0) =>
            _owner!.Memory.Span.Slice(_written);

        // ── IPooledFrame ─────────────────────────────────────────────────────

        /// <inheritdoc/>
        public ReadOnlySpan<byte> WrittenSpan => _owner!.Memory.Span.Slice(0, _written);

        /// <inheritdoc/>
        public int Length => _written;

        // ── IDisposable ──────────────────────────────────────────────────────

        /// <summary>Returns the underlying buffer to the pool. Safe to call multiple times.</summary>
        public void Dispose()
        {
            _owner?.Dispose();
            _owner = null;
        }
    }

    /// <summary>
    /// INTERNAL API.
    ///
    /// A lightweight <see cref="IPooledFrame"/> wrapper around a <see cref="ByteString"/>
    /// for use on the legacy (<c>zero-copy-codec = off</c>) write path. 🌸
    ///
    /// <para>
    /// <see cref="Dispose"/> is a no-op because <see cref="ByteString"/> is
    /// garbage-collected. This allows <see cref="PipeConnection"/> to use a single
    /// <c>Channel&lt;<see cref="IPooledFrame"/>&gt;</c> for both the legacy and the
    /// zero-copy paths with no hot-path branching in the write loop.
    /// </para>
    ///
    /// <!-- CopilotNotes: One extra wrapper object per message on the legacy path — negligible
    ///      overhead. The real cost on the legacy path remains the W5 ToByteString() allocation
    ///      inside AkkaProtocolHandle.Write; eliminating that is exactly what the zero-copy path
    ///      achieves. -->
    /// </summary>
    internal sealed class ByteStringPooledFrame : IPooledFrame
    {
        private readonly ByteString _payload;

        /// <summary>Wraps <paramref name="payload"/> as an <see cref="IPooledFrame"/>.</summary>
        public ByteStringPooledFrame(ByteString payload) => _payload = payload;

        /// <inheritdoc/>
        public ReadOnlySpan<byte> WrittenSpan => _payload.Span;

        /// <inheritdoc/>
        public int Length => _payload.Length;

        /// <inheritdoc/>
        /// <remarks>No-op — ByteString is GC-managed.</remarks>
        public void Dispose() { /* no-op */ }
    }
}

