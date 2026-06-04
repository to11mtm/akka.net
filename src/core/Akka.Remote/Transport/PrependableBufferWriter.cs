// -----------------------------------------------------------------------
//  <copyright file="PrependableBufferWriter.cs" company="Akka.NET Project">
//      Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
//      Copyright (C) 2013-2026 .NET Foundation <https://github.com/akkadotnet/akka.net>
//  </copyright>
// -----------------------------------------------------------------------

#nullable enable
using System;
using System.Buffers;
using System.Buffers.Binary;

namespace Akka.Remote.Transport;

public interface IPrependableBufferWriter : IBufferWriter<byte>, IDisposable
{
    Memory<byte> WrittenMemoryRange { get; }
    Memory<byte> PrependedMemoryRange { get; }
    Memory<byte> WrittenMemoryWithPrependRange { get; }
}

public abstract class PrependableArrayPoolBufferWriter : IPrependableBufferWriter
{
    private readonly ArrayPool<byte> _pool;
    protected byte[] _array;
    protected int _position;
    private readonly int _prependSize;

    public PrependableArrayPoolBufferWriter(ArrayPool<byte> pool, int prependableSize, int initialSize = 256)
    {
        _pool = pool;
        _prependSize = prependableSize;
        _position = prependableSize;
        _array = _pool.Rent(prependableSize|initialSize); // Start with a reasonable default size
    }
    
    public void Advance(int count)
    {
        _position += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        if (_position + sizeHint > _array.Length)
        {
            Grow(sizeHint);
        }
        return new(_array, _position, _array.Length - _position);
    }

    private void Grow(int sizeHint)
    {
        var newSize = Math.Max(_position + sizeHint, _array.Length * 2);
        var newArray = _pool.Rent(newSize);
        Array.Copy(_array, newArray, _position);
        _pool.Return(_array);
        _array = newArray;
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        return GetMemory(sizeHint).Span;
    }

    public void Dispose()
    {
        if (_array is not null)
            _pool.Return(_array);
    }

    public Memory<byte> WrittenMemoryRange => new(_array, _prependSize, _position - _prependSize);
    public Memory<byte> PrependedMemoryRange => new(_array, 0, _prependSize);
    public Memory<byte> WrittenMemoryWithPrependRange => WritePrependAndProvideBlock();
    
    protected abstract Memory<byte> WritePrependAndProvideBlock();
}

public class LittleEndian4ByteFrameArrayPoolBufferWriter : PrependableArrayPoolBufferWriter
{
    public LittleEndian4ByteFrameArrayPoolBufferWriter(ArrayPool<byte> pool, int initialSize = 256) 
        : base(pool, 4, initialSize)
    {
    }

    protected override Memory<byte> WritePrependAndProvideBlock()
    {
        BinaryPrimitives.WriteInt32LittleEndian(PrependedMemoryRange.Span, _position - 4);
        return new(_array, 0, _position);
    }
}