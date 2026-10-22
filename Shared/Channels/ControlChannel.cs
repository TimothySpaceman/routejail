using System.IO.Pipelines;
using StreamJsonRpc;

namespace Shared.Channels;

public class ControlChannel<TLocal, TRemote> : IAsyncDisposable where TLocal : class where TRemote : class
{
    private readonly JsonRpc _rpc;

    private ControlChannel(JsonRpc rpc, TRemote remote)
    {
        _rpc = rpc;
        Remote = remote;
    }

    public TRemote Remote { get; }

    public Task Completed => _rpc.Completion;

    public ValueTask DisposeAsync()
    {
        _rpc.Dispose();
        return default;
    }

    public static ControlChannel<TLocal, TRemote> Start(IDuplexPipe pipe, TLocal localHandler)
    {
        var messageHandler = new HeaderDelimitedMessageHandler(
            pipe.Output.AsStream(),
            pipe.Input.AsStream(),
            new SystemTextJsonFormatter()
        );

        var rpc = new JsonRpc(messageHandler);
        rpc.AddLocalRpcTarget(localHandler);
        var remote = rpc.Attach<TRemote>();
        rpc.StartListening();

        return new ControlChannel<TLocal, TRemote>(rpc, remote);
    }
}