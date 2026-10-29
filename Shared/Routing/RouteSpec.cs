using System.Text.Json.Serialization;

namespace Shared.Routing;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum RouteProtocol
{
    Tcp,
    Udp
}

public record RouteSpec(Guid Id, string Name, int Port, RouteProtocol Protocol = RouteProtocol.Tcp);