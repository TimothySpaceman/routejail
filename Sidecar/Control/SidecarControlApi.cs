using Shared.Control;
using Shared.Routing;
using Sidecar.Routing;

namespace Sidecar.Control;

public class SidecarControlApi(
    RouteInbox inbox,
    Func<Guid, string, int, RouteProtocol, CancellationToken, Task> acceptData
) : ISidecarControlApi
{
    public Task ApplyRoutes(RouteSnapshot s, CancellationToken ct)
    {
        inbox.Submit(s);
        return Task.CompletedTask;
    }

    public async Task AcceptDataConnection(
        Guid reservationId,
        string host,
        int port,
        RouteProtocol protocol,
        CancellationToken ct
    )
    {
        await acceptData(reservationId, host, port, protocol, ct);
    }
}