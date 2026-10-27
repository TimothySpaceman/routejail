using System.Buffers;
using System.Text;

namespace Shared.Frames;

public static class FrameCodec
{
    // Returns true and advances 'buffer' past the frame if a full frame is present.
    public static bool TryParse(ref ReadOnlySequence<byte> buffer, out Frame? frame)
    {
        var reader = new SequenceReader<byte>(buffer);

        if (!reader.TryReadTo(out ReadOnlySequence<byte> data, Frame.Terminator))
        {
            if (buffer.Length > Frame.MaxLength) throw new InvalidDataException("Frame too long");

            frame = null;
            return false;
        }

        frame = Parse(Encoding.UTF8.GetString(data));
        buffer = buffer.Slice(reader.Position);
        return true;
    }

    private static Frame Parse(string text)
    {
        var parts = text.Split(',');
        return parts[0] switch
        {
            "CONN" when parts.Length == 1 => new Frame.Connect(),
            "OPEN" when parts.Length == 2 && Guid.TryParse(parts[1], out var id) => new Frame.Open(id),
            _ => throw new InvalidDataException($"Bad frame: {text}")
        };
    }

    public static void Write(IBufferWriter<byte> writer, Frame frame)
    {
        var text = frame switch
        {
            Frame.Connect => "CONN;",
            Frame.Open o => $"OPEN,{o.RouteId};",
            _ => throw new ArgumentException("Unencodable frame")
        };

        var span = writer.GetSpan(Encoding.UTF8.GetMaxByteCount(text.Length));
        var n = Encoding.UTF8.GetBytes(text, span);
        writer.Advance(n);
    }
}