using System.IO.Pipelines;
using System.Net.Sockets;

namespace Shared.Transport;

public class Transport : ITransport
{
    private readonly Socket _socket;
    private readonly NetworkStream _stream;

    public Transport(Socket socket, bool isOwner)
    {
        _socket = socket;
        _stream = new NetworkStream(_socket, isOwner);

        var reader = PipeReader.Create(_stream, new StreamPipeReaderOptions(leaveOpen: true));
        var writer = PipeWriter.Create(_stream, new StreamPipeWriterOptions(leaveOpen: true));

        Input = new ShutdownOnCompleteReader(reader, () => Shutdown(SocketShutdown.Receive));
        Output = new ShutdownOnCompleteWriter(writer, () => Shutdown(SocketShutdown.Send));
    }

    public PipeReader Input { get; }
    public PipeWriter Output { get; }

    private void Shutdown(SocketShutdown how)
    {
        try { _socket.Shutdown(how); }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }
    }

    public async ValueTask DisposeAsync()
    {
        await Input.CompleteAsync();
        await Output.CompleteAsync();
        await _stream.DisposeAsync();
        _socket.Dispose();
    }
}

internal sealed class ShutdownOnCompleteWriter(PipeWriter inner, Action onComplete) : PipeWriter
{
    public override void Advance(int bytes) => inner.Advance(bytes);
    public override Memory<byte> GetMemory(int sizeHint = 0) => inner.GetMemory(sizeHint);
    public override Span<byte> GetSpan(int sizeHint = 0) => inner.GetSpan(sizeHint);
    public override void CancelPendingFlush() => inner.CancelPendingFlush();
    public override ValueTask<FlushResult> FlushAsync(CancellationToken ct = default) => inner.FlushAsync(ct);

    public override void Complete(Exception? exception = null)
    {
        inner.Complete(exception);
        onComplete();
    }

    public override async ValueTask CompleteAsync(Exception? exception = null)
    {
        await inner.CompleteAsync(exception);
        onComplete();
    }
}

internal sealed class ShutdownOnCompleteReader(PipeReader inner, Action onComplete) : PipeReader
{
    public override void AdvanceTo(SequencePosition consumed) => inner.AdvanceTo(consumed);
    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined) => inner.AdvanceTo(consumed, examined);
    public override void CancelPendingRead() => inner.CancelPendingRead();
    public override ValueTask<ReadResult> ReadAsync(CancellationToken ct = default) => inner.ReadAsync(ct);
    public override bool TryRead(out ReadResult result) => inner.TryRead(out result);

    public override void Complete(Exception? exception = null)
    {
        inner.Complete(exception);
        onComplete();
    }
    
    public override async ValueTask CompleteAsync(Exception? exception = null)
    {
        await inner.CompleteAsync(exception);
        onComplete();
    }
}