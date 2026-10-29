namespace Router.Peers;

public interface IPeerListenerManager
{
    Task StartListenerAsync(Guid peerId, string socketPath, CancellationToken ct = default);
    Task StopListenerAsync(Guid peerId);
    bool IsListenerRunning(Guid peerId);
}
