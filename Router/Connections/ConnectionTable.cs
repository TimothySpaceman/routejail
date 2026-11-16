using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Shared.Channels;
using Shared.Control;

namespace Router.Connections;

public sealed record ControlChannelEntry(
    ControlChannel<IRouterControlApi, ISidecarControlApi> Channel,
    DateTimeOffset ConnectedAt
);

public sealed class ConnectionTable
{
    public ConcurrentDictionary<Guid, ConnectionEntry> Connections { get; } = new();
    public ConcurrentDictionary<Guid, ControlChannelEntry> ControlChannels { get; } = new();
    public ConcurrentDictionary<Guid, PtpReservation> PtpReservations { get; } = new();

    public bool TryGetControlChannel(Guid peerId, [NotNullWhen(true)] out ControlChannelEntry? entry)
    {
        return ControlChannels.TryGetValue(peerId, out entry);
    }

    public bool TryGetConnection(Guid id, [NotNullWhen(true)] out ConnectionEntry? entry)
    {
        return Connections.TryGetValue(id, out entry);
    }
}
