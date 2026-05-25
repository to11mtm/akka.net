//-----------------------------------------------------------------------
// <copyright file="PooledCharBufferWriter.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable

using System;
using System.Buffers;

namespace Akka.Remote.Transport.Pipelines
{
    /// <summary>
    /// INTERNAL API.
    ///
    /// A growable <see cref="IBufferWriter{T}"/> of <see cref="char"/> backed by
    /// <see cref="ArrayPool{T}"/>-rented buffers. Used by the MessagePack codec to
    /// stream <c>ActorPath</c> chars into a pooled scratch before transcoding to
    /// UTF-8 — avoiding any intermediate <see cref="string"/> allocation.
    ///
    /// <para>
    /// Always call <see cref="Dispose"/> (or use a <c>using</c> statement) so the
    /// pooled buffer is returned. Forgetting to dispose leaks the rental but is
    /// otherwise harmless.
    /// </para>
    ///
    /// <!-- CopilotNotes: Mirrors the BCL ArrayBufferWriter shape but uses a
    ///      pooled char[] instead of an owned one. Geometric growth (2×) means
    ///      worst-case O(N) char copies on grow, but the steady-state hot path
    ///      sees zero copies for any path that fits in the initial 256-char rent. -->
    /// </summary>
    internal sealed class PooledCharBufferWriter : IBufferWriter<char>, IDisposable
    {
        private const int InitialCapacity = 256;

        private char[]? _buffer;
        private int _written;

        /// <summary>How many <see cref="char"/>s have been written so far.</summary>
        public int WrittenCount => _written;

        /// <summary>The written portion of the buffer as a read-only span.</summary>
        public ReadOnlySpan<char> WrittenSpan =>
            _buffer is null ? ReadOnlySpan<char>.Empty : _buffer.AsSpan(0, _written);

        /// <inheritdoc/>
        public void Advance(int count)
        {
            if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
            if (_buffer is null || _written + count > _buffer.Length)
                throw new InvalidOperationException("PooledCharBufferWriter: Advance beyond available capacity.");
            _written += count;
        }

        /// <inheritdoc/>
        public Memory<char> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer.AsMemory(_written);
        }

        /// <inheritdoc/>
        public Span<char> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _buffer.AsSpan(_written);
        }

        private void EnsureCapacity(int sizeHint)
        {
            if (sizeHint <= 0) sizeHint = 1;

            if (_buffer is null)
            {
                _buffer = ArrayPool<char>.Shared.Rent(Math.Max(sizeHint, InitialCapacity));
                return;
            }

            var available = _buffer.Length - _written;
            if (available >= sizeHint) return;

            // Geometric growth: max(2× current, exact need).
            var needed  = _written + sizeHint;
            var newSize = Math.Max(_buffer.Length * 2, needed);

            var newBuf = ArrayPool<char>.Shared.Rent(newSize);
            _buffer.AsSpan(0, _written).CopyTo(newBuf);
            ArrayPool<char>.Shared.Return(_buffer);
            _buffer = newBuf;
        }

        /// <summary>Returns the pooled buffer to <see cref="ArrayPool{T}.Shared"/>.</summary>
        public void Dispose()
        {
            if (_buffer is null) return;
            ArrayPool<char>.Shared.Return(_buffer);
            _buffer = null;
            _written = 0;
        }
    }
}

