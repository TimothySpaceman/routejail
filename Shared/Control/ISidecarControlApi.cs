namespace Shared.Control;

public interface ISidecarControlApi
{
    Task<int> GetSum(int a, int b);
}