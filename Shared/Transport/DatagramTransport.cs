using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Net.Sockets;

namespace Shared.Transport;

public sealed class DatagramTransport : ITransport
{
    public static readonly TimeSpan DefaultIdle = TimeSpan.FromSeconds(30);

    private static PipeOptions Options() => new(
        pauseWriterThreshold: 256 * 1024,
        resumeWriterThreshold: 128 * 1024,
        useSynchronizationContext: false
    );

    private readonly Pipe _in = new(Options());
    private readonly Pipe _out = new(Options());

    private readonly Func<CancellationToken, ValueTask<byte[]>> _receive;
    private readonly Func<byte[], CancellationToken, ValueTask> _send;
    private readonly TimeSpan _idle;
    private readonly IDisposable? _owned;
    private readonly CancellationTokenSource _cts;
    private Task _transferring = Task.CompletedTask;

    public PipeReader Input => _in.Reader;
    public PipeWriter Output => _out.Writer;
    public Task Transferring => _transferring;

    private DatagramTransport(
        Func<CancellationToken, ValueTask<byte[]>> receive,
        Func<byte[], CancellationToken, ValueTask> send,
        TimeSpan idle,
        CancellationToken ct,
        IDisposable? owned
    )
    {
        _receive = receive;
        _send = send;
        _idle = idle;
        _owned = owned;
        _cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        _cts.CancelAfter(idle);
    }

    public static DatagramTransport Start(
        Func<CancellationToken, ValueTask<byte[]>> receive,
        Func<byte[], CancellationToken, ValueTask> send,
        TimeSpan idle,
        CancellationToken ct,
        IDisposable? owned = null
    )
    {
        var t = new DatagramTransport(receive, send, idle, ct, owned);
        t._transferring = Task.WhenAll(t.ReceiveAsync(), t.SendAsync());
        return t;
    }

    // Connected UDP socket to host:port
    public static async Task<DatagramTransport> ConnectUdpAsync(
        string host,
        int port,
        TimeSpan idle,
        CancellationToken ct
    )
    {
        var udp = new Socket(SocketType.Dgram, ProtocolType.Udp);
        try
        {
            await udp.ConnectAsync(host, port, ct);
        }
        catch
        {
            udp.Dispose();
            throw;
        }

        var buffer = new byte[65535];
        return Start(
            async c =>
            {
                while (!c.IsCancellationRequested)
                {
                    try
                    {
                        var n = await udp.ReceiveAsync(buffer.AsMemory(), SocketFlags.None, c);
                        return buffer[..n];
                    }
                    catch (SocketException e) when (
                        e.SocketErrorCode is SocketError.ConnectionReset or SocketError.ConnectionRefused
                    )
                    {
                        // ICMP unreachable from the target, so we ignore it, idle timer decides
                    }
                }

                c.ThrowIfCancellationRequested();
                throw new InvalidOperationException("Unreachable");
            },
            async (data, c) =>
            {
                try
                {
                    await udp.SendAsync(data.AsMemory(), SocketFlags.None, c);
                }
                catch (SocketException)
                {
                }
            },
            idle, ct, udp
        );
    }
    
    private async Task ReceiveAsync()
    {
        var ct = _cts.Token;
        Exception? error = null;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var data = await _receive(ct);
                _cts.CancelAfter(_idle);

                var w = _in.Writer;
                var span = w.GetSpan(2 + data.Length);
                BinaryPrimitives.WriteUInt16BigEndian(span, (ushort)data.Length);
                data.CopyTo(span[2..]);
                w.Advance(2 + data.Length);

                // when the pipe is full this waits
                if ((await w.FlushAsync(ct)).IsCompleted) break;
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            error = ex;
        }
        finally
        {
            await _cts.CancelAsync();
            await _in.Writer.CompleteAsync(error);
        }
    }
    
    private async Task SendAsync()
    {
        var ct = _cts.Token;
        var r = _out.Reader;
        Exception? error = null;
        try
        {
            while (true)
            {
                var res = await r.ReadAsync(ct);
                var buffer = res.Buffer;
                try
                {
                    while (TryReadDatagram(ref buffer, out var data))
                    {
                        _cts.CancelAfter(_idle);
                        await _send(data, ct);
                    }
                }
                finally
                {
                    r.AdvanceTo(buffer.Start, buffer.End);
                }

                if (res.IsCompleted) break;
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            error = ex;
        }
        finally
        {
            await _cts.CancelAsync();
            await r.CompleteAsync(error);
        }
    }

    private static bool TryReadDatagram(ref ReadOnlySequence<byte> buffer, out byte[] payload)
    {
        payload = [];
        if (buffer.Length < 2) return false;

        Span<byte> lengthBytes = stackalloc byte[2];
        buffer.Slice(0, 2).CopyTo(lengthBytes);
        int length = BinaryPrimitives.ReadUInt16BigEndian(lengthBytes);
        if (buffer.Length < 2 + length) return false;

        payload = buffer.Slice(2, length).ToArray();
        buffer = buffer.Slice(2 + length);
        return true;
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        await _transferring;
        await _in.Reader.CompleteAsync();
        await _out.Writer.CompleteAsync();
        _owned?.Dispose();
        _cts.Dispose();
    }
}