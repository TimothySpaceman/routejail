using Shared.Routing;

namespace Router.Connections;

public record ConnectionDto(
    Guid Id,
    Guid PeerId,
    Guid? RouteId,
    string? RouteName,
    RouteProtocol Protocol,
    string Kind,
    Guid? DestinationPeerId,
    DateTimeOffset StartedAt,
    long BytesIn,
    long BytesOut,
    DateTimeOffset? LastActivity
)
{
    public Guid Peer => PeerId;
    public Guid? DestinationPeer => DestinationPeerId;
}
