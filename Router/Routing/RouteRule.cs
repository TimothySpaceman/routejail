using Shared.Routing;

namespace Router.Routing;

public sealed record RouteRule(
    Guid Id,
    Guid SourcePeerId,
    string Name,
    int Port,
    string Host,
    int TargetPort,
    Guid? DestinationPeerId = null,
    RouteProtocol Protocol = RouteProtocol.Tcp 
);
