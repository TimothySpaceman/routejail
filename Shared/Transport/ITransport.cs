using System.IO.Pipelines;

namespace Shared.Transport;

public interface ITransport : IDuplexPipe, IAsyncDisposable
{
}