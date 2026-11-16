using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Sidecar.Dns;

public sealed class DnsServer(ILogger<DnsServer> log) : BackgroundService
{
    private const int NoError = 0, FormErr = 1, NxDomain = 3, NotImp = 4;
    private const uint Ttl = 1;

    private readonly ConcurrentDictionary<string, IPAddress> _table = new(StringComparer.OrdinalIgnoreCase);

    public void Publish(string name, IPAddress ip)
    {
        _table[name] = ip;
        log.LogInformation("Domain {name} published at {ip}", name, ip);
    }
    public void Unpublish(string name) => _table.TryRemove(name, out _);
    public void Clear() => _table.Clear();

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        using var udp = new UdpClient(new IPEndPoint(IPAddress.Parse("127.0.0.53"), 53));
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var req = await udp.ReceiveAsync(ct);
                var resp = Answer(req.Buffer);
                if (resp is not null) await udp.SendAsync(resp, req.RemoteEndPoint, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "DNS query handling failed");
            }
        }
    }

    // name in table, type A     -> NOERROR, one A record
    // name in table, other type -> NOERROR, zero answers (AAAA must NOT be NXDOMAIN)
    // name not in table         -> NXDOMAIN
    private byte[]? Answer(byte[] question)
    {
        if (question.Length < 12) return null;
        var span = question.AsSpan();

        var flags = BinaryPrimitives.ReadUInt16BigEndian(span[2..]);
        if ((flags & 0x8000) != 0) return null;
        if (((flags >> 11) & 0xF) != 0) return Respond(question, 12, NotImp, null); // only standard QUERY
        if (BinaryPrimitives.ReadUInt16BigEndian(span[4..]) != 1)
            return Respond(question, 12, FormErr, null); // exactly one question

        // QNAME: length-prefixed labels ending in a zero byte.
        // Compression pointers (length >= 192) are not valid in a question, so they fall into the > 63 check.
        var name = new StringBuilder();
        var pos = 12;
        while (true)
        {
            if (pos >= question.Length) return Respond(question, 12, FormErr, null);
            int len = question[pos++];
            if (len == 0) break;
            if (len > 63 || pos + len > question.Length) return Respond(question, 12, FormErr, null);

            if (name.Length > 0) name.Append('.');
            name.Append(Encoding.ASCII.GetString(span.Slice(pos, len)));
            pos += len;
            if (name.Length > 253) return Respond(question, 12, FormErr, null);
        }

        if (pos + 4 > question.Length) return Respond(question, 12, FormErr, null); // QTYPE + QCLASS missing
        var qtype = BinaryPrimitives.ReadUInt16BigEndian(span[pos..]);
        var qclass = BinaryPrimitives.ReadUInt16BigEndian(span[(pos + 2)..]);
        var end = pos + 4; // end of the question section

        if (!_table.TryGetValue(name.ToString(), out var ip))
            return Respond(question, end, NxDomain, null);

        var v4 = ip.GetAddressBytes();
        var wantsA = qtype == 1 && qclass == 1 && v4.Length == 4; // A, class IN
        return Respond(question, end, NoError, wantsA ? v4 : null);
    }

    // Builds the response: request header + question copied verbatim (this also echoes the exact
    // case of the name, which resolvers using 0x20 randomisation check), then an optional A record.
    private static byte[] Respond(byte[] q, int end, int rcode, byte[]? ipv4)
    {
        var resp = new byte[end + (ipv4 is null ? 0 : 16)];
        q.AsSpan(0, end).CopyTo(resp); // keeps the query ID in bytes 0-1
        var s = resp.AsSpan();

        var reqFlags = BinaryPrimitives.ReadUInt16BigEndian(q.AsSpan(2));
        // QR=1, opcode copied, AA=1, TC=0, RD copied, RA=0, rcode
        var flags = (ushort)(0x8000 | (reqFlags & 0x7800) | 0x0400 | (reqFlags & 0x0100) | rcode);

        BinaryPrimitives.WriteUInt16BigEndian(s[2..], flags);
        BinaryPrimitives.WriteUInt16BigEndian(s[4..], (ushort)(end > 12 ? 1 : 0)); // QDCOUNT (0 for header-only errors)
        BinaryPrimitives.WriteUInt16BigEndian(s[6..], (ushort)(ipv4 is null ? 0 : 1)); // ANCOUNT
        BinaryPrimitives.WriteUInt16BigEndian(s[8..], 0); // NSCOUNT
        BinaryPrimitives.WriteUInt16BigEndian(s[10..], 0); // ARCOUNT: the request's EDNS OPT is dropped

        if (ipv4 is not null)
        {
            var rr = s[end..];
            rr[0] = 0xC0;
            rr[1] = 0x0C; // name = pointer to the question name at offset 12
            BinaryPrimitives.WriteUInt16BigEndian(rr[2..], 1); // TYPE A
            BinaryPrimitives.WriteUInt16BigEndian(rr[4..], 1); // CLASS IN
            BinaryPrimitives.WriteUInt32BigEndian(rr[6..], Ttl);
            BinaryPrimitives.WriteUInt16BigEndian(rr[10..], 4); // RDLENGTH
            ipv4.CopyTo(rr[12..]);
        }

        return resp;
    }
}