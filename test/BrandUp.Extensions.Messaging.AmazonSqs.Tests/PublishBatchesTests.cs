using BrandUp.Extensions.Messaging.Internals;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Where a batch is cut before it reaches the transport: by the number of messages a call may carry and
/// by the payload it may carry, whichever runs out first.
/// </summary>
public class PublishBatchesTests
{
    static (int Offset, int Count)[] Split(int[] sizes, int maxCount, int maxBytes)
        => [.. PublishBatches.Split(sizes, maxCount, maxBytes)];

    [Fact]
    public void Empty_ProducesNoCalls()
    {
        Assert.Empty(Split([], 10, 1000));
    }

    [Fact]
    public void FittingBatch_IsOneCall()
    {
        Assert.Equal([(0, 3)], Split([10, 10, 10], 10, 1000));
    }

    [Fact]
    public void CountLimit_CutsTheBatch()
    {
        Assert.Equal([(0, 2), (2, 2), (4, 1)], Split([1, 1, 1, 1, 1], maxCount: 2, maxBytes: 1000));
    }

    [Fact]
    public void ByteLimit_CutsTheBatch()
    {
        // 60 + 60 would be over the limit, so the second message starts a new call.
        Assert.Equal([(0, 1), (1, 2)], Split([60, 60, 30], maxCount: 10, maxBytes: 100));
    }

    [Fact]
    public void OversizedMessage_GoesAlone()
    {
        // Nothing here can split one message: it is sent on its own and the transport says what is wrong
        // with it, rather than this guessing.
        Assert.Equal([(0, 1), (1, 1), (2, 1)], Split([10, 500, 10], maxCount: 10, maxBytes: 100));
    }

    [Fact]
    public void EveryMessage_LandsInExactlyOneCall()
    {
        var sizes = Enumerable.Range(1, 47).Select(i => i * 7).ToArray();

        var batches = Split(sizes, maxCount: 4, maxBytes: 300);

        Assert.Equal(sizes.Length, batches.Sum(b => b.Count));
        var expectedOffset = 0;
        foreach (var (offset, count) in batches)
        {
            Assert.Equal(expectedOffset, offset);
            Assert.InRange(count, 1, 4);
            expectedOffset += count;
        }
    }
}
