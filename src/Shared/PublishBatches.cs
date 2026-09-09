// Linked as a shared source file into the transports that publish in batches (AmazonSqs, AmazonKinesis):
// both cut a batch the same way and report a partial failure the same way, only the limits differ.
namespace BrandUp.Extensions.Messaging.Internals;

internal static class PublishBatches
{
    /// <summary>
    /// Cuts <paramref name="sizes"/> into the batches one API call may carry: at most
    /// <paramref name="maxCount"/> messages, and at most <paramref name="maxBytes"/> of payload in
    /// total. Returned as ranges over the original list, so results can be mapped back by index.
    /// <para>
    /// A message larger than the whole limit is not split — nothing can be done about it here — and
    /// goes out alone, for the transport to reject with its own error rather than a guess of ours.
    /// </para>
    /// </summary>
    public static List<(int Offset, int Count)> Split(IReadOnlyList<int> sizes, int maxCount, int maxBytes)
    {
        ArgumentNullException.ThrowIfNull(sizes);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCount, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxBytes, 1);

        var batches = new List<(int Offset, int Count)>();
        var offset = 0;

        while (offset < sizes.Count)
        {
            var count = 1;
            var bytes = sizes[offset];

            while (offset + count < sizes.Count
                && count < maxCount
                && bytes + sizes[offset + count] <= maxBytes)
            {
                bytes += sizes[offset + count];
                count++;
            }

            batches.Add((offset, count));
            offset += count;
        }

        return batches;
    }

    /// <summary>
    /// The results of a finished batch — or the exception a partial failure is reported with, the same
    /// way for every transport.
    /// <para>
    /// A slot is empty exactly when the transport did not confirm that message: it either named it as
    /// failed, or said nothing about it at all. Both mean the same to the caller — that message is the
    /// one to publish again — so nothing else has to be tracked while the calls run.
    /// </para>
    /// </summary>
    /// <param name="results">One slot per message of the batch, filled where the transport confirmed it.</param>
    /// <param name="destination">Where it was published, as the message reads it: <c>queue 'orders'</c>.</param>
    /// <param name="errorCode">Provider code of the first failure, when it reported one.</param>
    public static IReadOnlyList<PublishResult> Complete(PublishResult?[] results, string destination, string? errorCode)
    {
        ArgumentNullException.ThrowIfNull(results);

        List<int>? failed = null;
        for (var i = 0; i < results.Length; i++)
        {
            if (results[i] is null)
                (failed ??= []).Add(i);
        }

        if (failed is null)
            return [.. results!];

        // Positions, not messages: the caller still holds the batch it passed in, and these index into it.
        var positions = string.Join(", ", failed.Take(10)) + (failed.Count > 10 ? ", …" : "");

        throw new BatchPublishException(
            $"Failed to publish {failed.Count} of {results.Length} messages to {destination} " +
            $"(positions {positions}); the rest were published.",
            errorCode,
            failed);
    }
}
