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

    public Task Transferring { get; private set; } = Task.CompletedTask;

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
            TransferData(_peerA.Input, _peerB.Output, _cts),
            TransferData(_peerB.Input, _peerA.Output, _cts)
        );
    }

    private async Task TransferData(PipeReader source, PipeWriter destination, CancellationTokenSource abort)
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

                foreach (var segment in result.Buffer)
                {
                    destination.Write(segment.Span);
                }

                source.AdvanceTo(result.Buffer.End);

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