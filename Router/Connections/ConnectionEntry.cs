using Shared.Channels;
using Shared.Routing;

namespace Router.Connections;

public enum ConnectionKind
{
    Direct,
    Ptp
}

public sealed class ConnectionEntry
{
    public ConnectionEntry(Guid id, Guid peerId)
    {
        Id = id;
        PeerId = peerId;
        StartedAt = DateTimeOffset.UtcNow;
    }

    public Guid Id { get; }
    public Guid PeerId { get; }
    public Guid? RouteId { get; set; }
    public string? RouteName { get; set; }
    public RouteProtocol? Protocol { get; set; }
    public ConnectionKind Kind { get; set; } = ConnectionKind.Direct;
    public Guid? DestinationPeerId { get; set; }

    public string? TargetHost { get; set; }
    public int? TargetPort { get; set; }

    public DateTimeOffset StartedAt { get; set; }
    public Task Running { get; set; } = Task.CompletedTask;
    private volatile DataChannel? _channel;

    public DataChannel? Channel
    {
        get => _channel;
        set => _channel = value;
    }

    public CancellationTokenSource? Cts { get; set; }
    public ConnectionEntry? Counterpart { get; set; }
    public bool IsCounterpart { get; set; }

    public void Kill()
    {
        try
        {
            Cts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        try
        {
            Counterpart?.Cts?.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
    }
}
