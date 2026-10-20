namespace Shared.Frames;

public abstract record Frame
{
    public const byte Terminator = (byte)';';
    public const int MaxLength = 265;

    // CONN;
    // 4 + 1 = 5 
    // Requests first (= Control) channel
    public sealed record Connect : Frame;

    // OPEN,hostname,port;
    // 4 +  <=255 +  <=5+1 = up to 265 
    // Requests data channel
    public sealed record Open(string Host, int Port) : Frame;
}