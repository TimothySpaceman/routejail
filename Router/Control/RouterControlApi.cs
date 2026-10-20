using Shared.Control;

namespace Router.Control;

public class RouterControlApi : IRouterControlApi
{
    public async Task<int> GetRandomNumberFromRouter(CancellationToken ct)
    {
        return new Random().Next(1024);
    }
}