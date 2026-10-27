using Shared.Control;
using Shared.Routing;
using Sidecar.Routing;

namespace Sidecar.Control;

public class SidecarControlApi(RouteInbox inbox) : ISidecarControlApi
{
    public Task ApplyRoutes(RouteSnapshot s)
    {
        inbox.Submit(s);
        return Task.CompletedTask;
    }
}