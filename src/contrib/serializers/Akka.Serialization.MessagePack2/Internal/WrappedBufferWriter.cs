//-----------------------------------------------------------------------
// <copyright file="WrappedBufferWriter.cs" company="Akka.NET Project">
//     Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//     Copyright (C) 2013-2025 .NET Foundation <https://github.com/akkadotnet/akka.net>
// </copyright>
//-----------------------------------------------------------------------

#nullable enable
using System;
using System.Buffers;
using Microsoft.Extensions.ObjectPool;

namespace Akka.Serialization.MessagePack.Internal
{
    /// <summary>
    /// A thin decorator around an arbitrary <see cref="IBufferWriter{T}"/> that reliably tracks how
    /// many bytes have been written through it. UwU~ ✨
    ///
    /// <para>
    /// Unlike peeking at <see cref="ArrayBufferWriter{T}.WrittenCount"/> — which only works for one
    /// concrete writer type — this wrapper observes every <see cref="Advance"/> call and accumulates
    /// the exact payload size for <em>any</em> underlying writer. (◕‿◕)
    /// </para>
    /// </summary>
    /// <remarks>
    /// CopilotNote: this is a <see langword="sealed"/> reference type (not a struct) on purpose —
    /// <see cref="IBufferWriter{T}"/> consumers may capture/box the instance, and a struct would lose
    /// its <see cref="BytesWritten"/> mutations across such boundaries. 🛟
    /// </remarks>
    internal sealed class WrappedBufferWriter : IBufferWriter<byte>
    {
        private readonly IBufferWriter<byte> _inner;

        /// <summary>
        /// Initializes a new instance of the <see cref="WrappedBufferWriter"/> class.
        /// </summary>
        /// <param name="inner">The underlying writer to forward all buffer requests to.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="inner"/> is <see langword="null"/>.</exception>
        public WrappedBufferWriter(IBufferWriter<byte> inner)
            => _inner = inner ?? throw new ArgumentNullException(nameof(inner));

        /// <summary>
        /// The total number of bytes committed through this wrapper via <see cref="Advance"/>.
        /// </summary>
        public long BytesWritten { get; private set; }

        /// <inheritdoc />
        public void Advance(int count)
        {
            _inner.Advance(count);
            BytesWritten += count;
        }

        /// <inheritdoc />
        public Memory<byte> GetMemory(int sizeHint = 0)
            => _inner.GetMemory(sizeHint);

        /// <inheritdoc />
        public Span<byte> GetSpan(int sizeHint = 0)
            => _inner.GetSpan(sizeHint);
    }
}

