namespace Shared.Control;

public interface IRouterControlApi
{
    Task<int> GetRandomNumberFromRouter(CancellationToken ct);
}