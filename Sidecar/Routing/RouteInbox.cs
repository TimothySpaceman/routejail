using System.Threading.Channels;
using Shared.Routing;

namespace Sidecar.Routing;

public class RouteInbox
{
    private readonly Channel<RouteSnapshot> _ch = Channel.CreateBounded<RouteSnapshot>(
        new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropOldest, SingleReader = true
        }
    );

    private readonly Lock _gate = new();
    private long _version = -1;

    public void Submit(RouteSnapshot s)
    {
        lock (_gate)
        {
            if (s.Version <= _version) return; // stale or duplicate
            _version = s.Version;
            _ch.Writer.TryWrite(s);
        }
    }

    public IAsyncEnumerable<RouteSnapshot> ReadAllAsync(CancellationToken ct) => _ch.Reader.ReadAllAsync(ct);
}