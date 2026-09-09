using System.Collections.Concurrent;
using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Dispatch semantics of the hosted consumer, driven by a stub queue: ordering, failure handling and
/// what ends up deleted. No SQS involved — this is the library's own logic.
/// </summary>
public class ConsumerDispatchTests
{
    static async Task<(StubQueue Queue, List<string> Handled)> RunAsync(
        IEnumerable<ReceivedMessage<TestMessage>> batch, Func<TestMessage, bool> succeeds, int maxConcurrency = 1)
    {
        var queue = new StubQueue([.. batch]);
        var handled = new List<string>();

        var services = new ServiceCollection();
        services.AddScoped<IMessageHandler<TestMessage>>(_ => new StubHandler(handled, succeeds));
        await using var provider = services.BuildServiceProvider();

        var options = new SqsConsumerOptions { MaxConcurrency = maxConcurrency, WaitTime = TimeSpan.Zero };
        var consumer = new SqsConsumerService<TestMessage>(
            queue,
            provider.GetRequiredService<IServiceScopeFactory>(),
            new StubOptionsMonitor(options),
            NullLogger<SqsConsumerService<TestMessage>>.Instance);

        using var stopping = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        await ((IHostedService)consumer).StartAsync(stopping.Token);
        await queue.Drained.WaitAsync(stopping.Token);
        await consumer.StopAsync(CancellationToken.None);

        return (queue, handled);
    }

    [Fact]
    public async Task FifoGroup_StopsAfterFailure_LaterMessagesAreLeftForRedelivery()
    {
        var batch = new[] { Message("m1", "g1"), Message("m2", "g1"), Message("m3", "g1") };

        var (queue, handled) = await RunAsync(batch, message => message.Id != "m1");

        // m1 failed, so nothing later in its group may overtake it.
        Assert.Empty(handled);
        Assert.Empty(queue.Deleted);
    }

    [Fact]
    public async Task FifoGroup_ProcessedInOrder()
    {
        var batch = new[] { Message("m1", "g1"), Message("m2", "g1"), Message("m3", "g1") };

        var (queue, handled) = await RunAsync(batch, _ => true, maxConcurrency: 8);

        Assert.Equal(["m1", "m2", "m3"], handled);
        Assert.Equal(["m1", "m2", "m3"], queue.Deleted);
    }

    [Fact]
    public async Task FifoGroups_AreIndependent_OneFailureDoesNotBlockAnother()
    {
        var batch = new[] { Message("a1", "ga"), Message("a2", "ga"), Message("b1", "gb") };

        var (queue, handled) = await RunAsync(batch, message => message.Id != "a1");

        // Group ga stops at a1; group gb is unaffected.
        Assert.Equal(["b1"], handled);
        Assert.Equal(["b1"], queue.Deleted);
    }

    [Fact]
    public async Task StandardQueue_FailureDoesNotBlockOtherMessages()
    {
        var batch = new[] { Message("m1"), Message("m2"), Message("m3") };

        var (queue, handled) = await RunAsync(batch, message => message.Id != "m1");

        // No groups: every message is independent, only the failed one is left behind.
        Assert.Equal(["m2", "m3"], [.. handled.Order()]);
        Assert.Equal(["m2", "m3"], [.. queue.Deleted.Order()]);
    }

    static ReceivedMessage<TestMessage> Message(string id, string? groupId = null)
        => new()
        {
            MessageId = id,
            Body = new TestMessage { Id = id },
            ReceiptHandle = id,
            GroupId = groupId,
        };

    public class TestMessage
    {
        public string Id { get; set; } = "";
    }

    class StubHandler(List<string> handled, Func<TestMessage, bool> succeeds) : IMessageHandler<TestMessage>
    {
        public Task HandleAsync(MessageContext<TestMessage> context, CancellationToken cancellationToken)
        {
            if (!succeeds(context.Message))
                throw new InvalidOperationException($"handler failed for {context.Message.Id}");

            lock (handled)
                handled.Add(context.Message.Id);

            return Task.CompletedTask;
        }
    }

    /// <summary>Hands out one batch, then blocks — so the test sees exactly one dispatch cycle.</summary>
    class StubQueue(List<ReceivedMessage<TestMessage>> batch) : IMessageQueue<TestMessage>
    {
        readonly TaskCompletionSource drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
        int delivered;

        public Task Drained => drained.Task;
        public ConcurrentQueue<string> DeletedIds { get; } = new();
        public IReadOnlyList<string> Deleted => [.. DeletedIds];

        public string Name => "stub";

        public Task<IReadOnlyList<PublishResult>> PublishAsync(
            IReadOnlyCollection<PublishMessage<TestMessage>> messages, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<ReceivedMessage<TestMessage>>> ReceiveAsync(int maxMessages = 1, TimeSpan? waitTime = null, CancellationToken cancellationToken = default)
        {
            if (Interlocked.Exchange(ref delivered, 1) == 0)
                return Task.FromResult<IReadOnlyList<ReceivedMessage<TestMessage>>>(batch);

            // The batch is done: release the test, then idle until the consumer is stopped.
            drained.TrySetResult();
            return Task.Delay(Timeout.Infinite, cancellationToken)
                .ContinueWith(_ => (IReadOnlyList<ReceivedMessage<TestMessage>>)[], TaskScheduler.Default);
        }

        public Task DeleteAsync(ReceivedMessage<TestMessage> message, CancellationToken cancellationToken = default)
        {
            DeletedIds.Enqueue(message.MessageId);
            return Task.CompletedTask;
        }

        public Task DeleteAsync(IReadOnlyCollection<ReceivedMessage<TestMessage>> messages, CancellationToken cancellationToken = default)
        {
            foreach (var message in messages)
                DeletedIds.Enqueue(message.MessageId);
            return Task.CompletedTask;
        }

        public Task AbandonAsync(ReceivedMessage<TestMessage> message, TimeSpan? delay = null, CancellationToken cancellationToken = default)
            => Task.CompletedTask;

        public Task<PublishResult> PublishAsync(TestMessage message, PublishOptions? options = null, CancellationToken cancellationToken = default)
            => throw new NotSupportedException();

        public Task<bool> ExistsAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);
        public Task<int> GetApproximateCountAsync(CancellationToken cancellationToken = default) => Task.FromResult(0);
        public Task PurgeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
    }

    class StubOptionsMonitor(SqsConsumerOptions options) : IOptionsMonitor<SqsConsumerOptions>
    {
        public SqsConsumerOptions CurrentValue => options;
        public SqsConsumerOptions Get(string? name) => options;
        public IDisposable? OnChange(Action<SqsConsumerOptions, string?> listener) => null;
    }
}
