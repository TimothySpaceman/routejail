using System.Buffers;
using System.IO.Pipelines;
using Shared.Transport;

namespace Router.Channels;

public class DataChannel
{
    private readonly ITransport _peerA;
    private readonly ITransport _peerB;

    public Task Transferring { get; private set; } = Task.CompletedTask;

    private DataChannel(ITransport peerA, ITransport peerB)
    {
        _peerA = peerA;
        _peerB = peerB;
    }

    public static DataChannel Start(ITransport peerA, ITransport peerB, CancellationToken ct)
    {
        var channel = new DataChannel(peerA, peerB);
        channel.Transferring = channel.RunAsync(ct);
        return channel;
    }

    private async Task RunAsync(CancellationToken ct)
    {
        // Cancelled when either direction faults, or when the caller cancels
        using var abort = CancellationTokenSource.CreateLinkedTokenSource(ct);

        await Task.WhenAll(
            TransferData(_peerA.Input, _peerB.Output, abort),
            TransferData(_peerB.Input, _peerA.Output, abort)
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
}