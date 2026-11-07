using System.Buffers;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;

namespace Sidecar.Relay;

public sealed class SocketRelay
{
    private const int BufferSize = 64 * 1024;

    private long _bytesAToB;
    private long _bytesBToA;

    private Exception? _error;

    public async Task RunAsync(Socket a, Socket b, CancellationToken ct)
    {
        if (a.ProtocolType == ProtocolType.Tcp) a.NoDelay = true;
        if (b.ProtocolType == ProtocolType.Tcp) b.NoDelay = true;

        ActiveConnections.Enter();
        try
        {
            using var abort = CancellationTokenSource.CreateLinkedTokenSource(ct);
            await Task.WhenAll(
                PumpAsync(a, b, abort, isAToB: true),
                PumpAsync(b, a, abort, isAToB: false)
            );
        }
        finally
        {
            ActiveConnections.Exit();
        }

        var error = Volatile.Read(ref _error);
        if (error is not null) ExceptionDispatchInfo.Throw(error);
    }

    private async Task PumpAsync(Socket source, Socket destination, CancellationTokenSource abort, bool isAToB)
    {
        var ct = abort.Token;
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        try
        {
            while (true)
            {
                var n = await source.ReceiveAsync(buffer.AsMemory(0, BufferSize), SocketFlags.None, ct);
                if (n == 0)
                {
                    Shutdown(destination, SocketShutdown.Send);
                    return;
                }

                var sent = 0;
                while (sent < n)
                {
                    sent += await destination.SendAsync(buffer.AsMemory(sent, n - sent), SocketFlags.None, ct);
                }

                if (isAToB) Interlocked.Add(ref _bytesAToB, n);
                else Interlocked.Add(ref _bytesBToA, n);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Interlocked.CompareExchange(ref _error, ex, null);
            await abort.CancelAsync();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static void Shutdown(Socket socket, SocketShutdown how)
    {
        try
        {
            socket.Shutdown(how);
        }
        catch (SocketException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
    }
}