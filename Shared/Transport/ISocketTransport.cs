using System.IO.Pipelines;

namespace Shared.Transport;

public interface ISocketTransport : IDuplexPipe, IAsyncDisposable
{
}