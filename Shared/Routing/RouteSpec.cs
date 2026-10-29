namespace Shared.Routing;

public enum RouteProtocol
{
    Tcp,
    Udp
}

public record RouteSpec(Guid Id, string Name, int Port, RouteProtocol Protocol = RouteProtocol.Tcp);