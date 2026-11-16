using System.Net;

namespace Sidecar.Dns;

using System;
using System.Collections.Generic;
using System.Net;

class LoopbackAllocator
{
    private const uint Min = 2;
    private const uint Max = 65534;

    private uint _next = Min;
    private readonly HashSet<uint> _used = new();

    public IPAddress Rent()
    {
        uint start = _next;

        do
        {
            uint candidate = _next;

            _next = _next >= Max ? Min : _next + 1;

            if (_used.Add(candidate))
            {
                return new IPAddress(new byte[] { 127, 0, (byte)(candidate >> 8), (byte)candidate });
            }
        } while (_next != start);

        throw new InvalidOperationException("No loopback addresses available");
    }

    public void Return(IPAddress ip)
    {
        byte[] bytes = ip.GetAddressBytes();

        if (bytes.Length != 4 || bytes[0] != 127 || bytes[1] != 0)
        {
            throw new ArgumentException(
                "Address must belong to the 127.0.0.0/16 subnet",
                nameof(ip)
            );
        }

        uint value = ((uint)bytes[2] << 8) | bytes[3];

        if (value < Min || value > Max)
        {
            throw new ArgumentException(
                "Address is outside the allocatable range",
                nameof(ip)
            );
        }

        _used.Remove(value);
    }
}