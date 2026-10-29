using Shared.Routing;

namespace Router.Routing;

public record RouteRuleDto(
    Guid Id,
    Guid SourcePeerId,
    string Name,
    int Port,
    string Host,
    int TargetPort,
    Guid? DestinationPeerId = null,
    RouteProtocol Protocol = RouteProtocol.Tcp
);

public record CreateRouteRuleRequest(
    Guid? Id,
    Guid SourcePeerId,
    string Name,
    int Port,
    string Host,
    int TargetPort,
    Guid? DestinationPeerId = null,
    RouteProtocol Protocol = RouteProtocol.Tcp
);

public record UpdateRouteRuleRequest(
    Guid SourcePeerId,
    string Name,
    int Port,
    string Host,
    int TargetPort,
    Guid? DestinationPeerId = null,
    RouteProtocol Protocol = RouteProtocol.Tcp
);
