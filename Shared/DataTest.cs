using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Shared.Transport;

namespace Shared;

// TEMP. Just for testing
public static class Producer
{
    public static async Task RunAsync(ITransport t, int totalBytes, CancellationToken ct)
    {
        var rng = new Random(1);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        for (var sent = 0; sent < totalBytes;)
        {
            var size = Math.Min(rng.Next(1, 64 * 1024), totalBytes - sent);
            WriteChunk(t.Output, rng, hash, size);
            await t.Output.FlushAsync(ct);
            sent += size;
        }

        var expected = hash.GetHashAndReset();
        await t.Output.CompleteAsync();

        var reply = new List<byte>();
        while (true)
        {
            var result = await t.Input.ReadAsync(ct);
            foreach (var seg in result.Buffer) reply.AddRange(seg.Span.ToArray());
            t.Input.AdvanceTo(result.Buffer.End);
            if (result.IsCompleted) break;
        }

        Console.WriteLine(reply.SequenceEqual(expected)
            ? $"Producer: OK, {totalBytes} bytes, hash confirmed by consumer"
            : "Producer: FAIL, hash reply missing or mismatched");
    }

    private static void WriteChunk(PipeWriter output, Random rng, IncrementalHash hash, int size)
    {
        var span = output.GetSpan(size)[..size];
        rng.NextBytes(span);
        hash.AppendData(span);
        output.Advance(size);
    }
}

public static class Consumer
{
    public static async Task RunAsync(int port, CancellationToken ct)
    {
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        var socket = await listener.AcceptSocketAsync(ct);
        await using var t = new Transport.Transport(socket, true);

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        long received = 0;

        while (true)
        {
            var result = await t.Input.ReadAsync(ct);
            foreach (var seg in result.Buffer) hash.AppendData(seg.Span);
            received += result.Buffer.Length;
            t.Input.AdvanceTo(result.Buffer.End);
            if (result.IsCompleted) break;
        }

        Console.WriteLine($"Consumer: EOF after {received} bytes, replying with hash");
        t.Output.Write(hash.GetHashAndReset());
        await t.Output.FlushAsync(ct);
        await t.Output.CompleteAsync();
    }
}