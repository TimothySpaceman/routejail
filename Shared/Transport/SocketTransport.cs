using System.IO.Pipelines;
using System.Net.Sockets;

namespace Shared.Transport;

public class SocketTransport : ISocketTransport
{
    private readonly Socket _socket;
    private readonly NetworkStream _stream;

    public SocketTransport(Socket socket, bool isOwner)
    {
        _socket = socket;
        _stream = new NetworkStream(_socket, isOwner);
        Input = PipeReader.Create(_stream);
        Output = PipeWriter.Create(_stream);
    }

    public PipeReader Input { get; }
    public PipeWriter Output { get; }

    public async ValueTask DisposeAsync()
    {
        await Input.CompleteAsync();
        await Output.CompleteAsync();
        await _stream.DisposeAsync();
        _socket.Dispose();
    }
}