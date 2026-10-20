using Shared.Control;

namespace Sidecar.Control;

public class SidecarControlApi : ISidecarControlApi
{
    public async Task<int> GetSum(int a, int b)
    {
        return a + b;
    }
}