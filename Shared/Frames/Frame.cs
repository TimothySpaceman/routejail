namespace Shared.Frames;

public abstract record Frame
{
    public const char Terminator = ';';
    public const char Delimiter = ',';
    public const int MaxLength = 44;

    // CONNECT;
    // 7 +    1 = 8 
    // Requests first (= Control) Channel
    public sealed record Connect : Frame;

    // OPEN,route_id;
    // 4 + 1 + 36 + 1 = 42
    // Requests Data Channel
    public sealed record Open(Guid RouteId) : Frame;

    // ACCEPT,reservation_id;
    // 6 +   1 + 36 +       1 = 44 
    // Accepts Data Channel as a target peer
    public sealed record Accept(Guid ReservationId) : Frame;
}