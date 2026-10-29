using System.Buffers;
using System.IO.Pipelines;
using Shared.Transport;

namespace Shared.Channels;

public class DataChannel : IAsyncDisposable
{
    private readonly ITransport _peerA;
    private readonly ITransport _peerB;
    private readonly CancellationTokenSource _cts;
    private int _disposed;
    private long _bytesIn;
    private long _bytesOut;
    private long _lastActivityTicks;

    public Task Transferring { get; private set; } = Task.CompletedTask;

    public long BytesIn => Volatile.Read(ref _bytesIn);
    public long BytesOut => Volatile.Read(ref _bytesOut);
    public DateTimeOffset? LastActivity
    {
        get
        {
            var ticks = Volatile.Read(ref _lastActivityTicks);
            return ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    private DataChannel(ITransport peerA, ITransport peerB, CancellationTokenSource cts)
    {
        _peerA = peerA;
        _peerB = peerB;
        _cts = cts;
    }

    public static DataChannel Start(ITransport peerA, ITransport peerB, CancellationToken ct)
    {
        var channel = new DataChannel(peerA, peerB, CancellationTokenSource.CreateLinkedTokenSource(ct));
        channel.Transferring = channel.RunAsync();
        return channel;
    }

    private async Task RunAsync()
    {
        await Task.WhenAll(
            TransferData(_peerA.Input, _peerB.Output, _cts, isDirectionIn: true),
            TransferData(_peerB.Input, _peerA.Output, _cts, isDirectionIn: false)
        );
    }

    private async Task TransferData(PipeReader source, PipeWriter destination, CancellationTokenSource abort, bool isDirectionIn)
    {
        // Might access some Channel State like interference config to interact with the data being transfered
        // Thus, cannot be static
        var ct = abort.Token;
        Exception? error = null;
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var result = await source.ReadAsync(ct);
                if (result.IsCanceled) break;

                var bytes = result.Buffer.Length;
                foreach (var segment in result.Buffer)
                {
                    destination.Write(segment.Span);
                }

                source.AdvanceTo(result.Buffer.End);

                if (bytes > 0)
                {
                    if (isDirectionIn)
                    {
                        Interlocked.Add(ref _bytesIn, bytes);
                    }
                    else
                    {
                        Interlocked.Add(ref _bytesOut, bytes);
                    }
                    Interlocked.Exchange(ref _lastActivityTicks, DateTimeOffset.UtcNow.UtcTicks);
                }

                var flush = await destination.FlushAsync(ct);
                if (flush.IsCanceled || flush.IsCompleted || result.IsCompleted) break;
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Aborted by the caller or by the other direction faulting
        }
        catch (Exception ex)
        {
            error = ex;
            await abort.CancelAsync(); // take down the other direction too
            throw;
        }
        finally
        {
            // EOF (error == null) propagates as a send-side shutdown to the peer
            await destination.CompleteAsync(error);
            await source.CompleteAsync(error);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        _cts.Cancel();

        try
        {
            await Transferring;
        }
        catch
        {
            // already observed by whoever awaited Transferring
        }

        _cts.Dispose();
    }
}