using Shared.Routing;

namespace Shared.Control;

public interface ISidecarControlApi
{
    Task ApplyRoutes(RouteSnapshot snapshot, CancellationToken ct);
    Task AcceptDataConnection(Guid reservationId, string host, int port, CancellationToken ct);
}