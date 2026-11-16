using Shared.Transport;

namespace Router.Connections;

public sealed record PtpReservation(Guid Id, ITransport SourceTransport, Guid SourceConnectionId, Guid DestinationPeerId)
{
    public TaskCompletionSource Tsc { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
}
