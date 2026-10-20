using System.IO.Pipelines;

namespace Shared.Frames;

public static class FrameReader
{
    public static async Task<Frame> ReadFrameAsync(PipeReader input, CancellationToken ct)
    {
        while (true)
        {
            var result = await input.ReadAsync(ct);
            var buffer = result.Buffer;

            if (FrameCodec.TryParse(ref buffer, out var frame))
            {
                input.AdvanceTo(buffer.Start);
                return frame!;
            }

            input.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted) throw new IOException("Closed mid-frame");
        }
    }
}