using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace Shared.Transport;

public sealed class Transport : ITransport
{
    private const int MinReadSize = 64 * 1024; // also the pipe segment size
    private const int PauseWriterThreshold = 1024 * 1024;
    private const int ResumeWriterThreshold = 512 * 1024;

    private static readonly PipeOptions InputOptions = new(
        pauseWriterThreshold: PauseWriterThreshold,
        resumeWriterThreshold: ResumeWriterThreshold,
        minimumSegmentSize: MinReadSize,
        readerScheduler: PipeScheduler.ThreadPool,
        writerScheduler: PipeScheduler.Inline,
        useSynchronizationContext: false
    );

    private static readonly PipeOptions OutputOptions = new(
        pauseWriterThreshold: PauseWriterThreshold,
        resumeWriterThreshold: ResumeWriterThreshold,
        minimumSegmentSize: MinReadSize,
        readerScheduler: PipeScheduler.Inline,
        writerScheduler: PipeScheduler.ThreadPool,
        useSynchronizationContext: false
    );

    private readonly Socket _socket;
    private readonly bool _isOwner;
    private readonly Pipe _input = new(InputOptions);
    private readonly Pipe _output = new(OutputOptions);
    private readonly List<ArraySegment<byte>> _segments = new();
    private readonly Task _receiving;
    private readonly Task _sending;
    private int _disposed;

    public Transport(Socket socket, bool isOwner)
    {
        _socket = socket;
        _isOwner = isOwner;
        if (socket.ProtocolType == ProtocolType.Tcp) socket.NoDelay = true;

        _receiving = ReceiveLoopAsync();
        _sending = SendLoopAsync();
    }

    public PipeReader Input => _input.Reader;
    public PipeWriter Output => _output.Writer;

    private async Task ReceiveLoopAsync()
    {
        var writer = _input.Writer;
        Exception? error = null;
        try
        {
            while (true)
            {
                var memory = writer.GetMemory(MinReadSize);
                var n = await _socket.ReceiveAsync(memory, SocketFlags.None);
                if (n == 0) break;

                writer.Advance(n);
                var flush = await writer.FlushAsync(); // backpressure
                if (flush.IsCompleted) break; // app completed Input
            }
        }
        catch (ObjectDisposedException)
        {
        }
        catch (Exception ex)
        {
            error = ex;
        }
        finally
        {
            Shutdown(SocketShutdown.Receive);
            await writer.CompleteAsync(error);
        }
    }

    private async Task SendLoopAsync()
    {
        var reader = _output.Reader;
        Exception? error = null;
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync();
                var buffer = result.Buffer;

                if (!buffer.IsEmpty) await SendAsync(buffer);
                reader.AdvanceTo(buffer.End);

                if (result.IsCompleted || result.IsCanceled) break;
            }

            Shutdown(SocketShutdown.Send);
        }
        catch (Exception ex)
        {
            error = ex;
            Shutdown(SocketShutdown.Both);
        }
        finally
        {
            await reader.CompleteAsync(error);
        }
    }

    private async ValueTask SendAsync(ReadOnlySequence<byte> buffer)
    {
        if (buffer.IsSingleSegment)
        {
            await _socket.SendAsync(buffer.First, SocketFlags.None);
            return;
        }

        _segments.Clear();
        foreach (var memory in buffer)
        {
            if (!MemoryMarshal.TryGetArray(memory, out var segment))
                throw new InvalidOperationException("Pipe memory is not array-backed");
            _segments.Add(segment);
        }

        await _socket.SendAsync(_segments, SocketFlags.None);
    }

    private void Shutdown(SocketShutdown how)
    {
        try
        {
            _socket.Shutdown(how);
        }
        catch (SocketException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        await _output.Writer.CompleteAsync();
        await _input.Reader.CompleteAsync();

        try
        {
            await _sending.WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch
        {
            // peer not reading or send failed
        }

        Shutdown(SocketShutdown.Both);

        try
        {
            await _receiving;
        }
        catch
        {
            // ignored
        }

        try
        {
            await _sending;
        }
        catch
        {
            // ignored
        }

        if (_isOwner) _socket.Dispose();
    }
}