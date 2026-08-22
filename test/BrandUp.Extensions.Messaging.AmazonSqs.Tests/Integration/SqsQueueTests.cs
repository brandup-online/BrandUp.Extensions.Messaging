using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging.Integration;

/// <summary>
/// End-to-end tests against an SQS-compatible server (ElasticMQ in CI). Queue names carry a unique
/// suffix per test run, so parallel runs do not see each other's messages.
/// </summary>
[Trait("Category", "Integration")]
public class SqsQueueTests
{
    static ServiceProvider CreateProvider(Action<SqsMessagingBuilder> registerQueues)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        var builder = services.AddSqsMessaging(options =>
        {
            SqsEnvironment.Apply(options);
            options.QueueNameSuffix = "-" + Guid.NewGuid().ToString("N")[..8];
        });
        registerQueues(builder);
        return services.BuildServiceProvider();
    }

    [SqsFact]
    public async Task PublishReceiveDelete_Roundtrip()
    {
        await using var provider = CreateProvider(builder => builder.AddQueue<TestOrder>("test-orders"));
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        var message = new TestOrder { OrderId = Guid.NewGuid(), Total = 42.5m };
        var publishResult = await queue.PublishAsync(message, new PublishOptions { Attributes = { ["origin"] = "test" } });
        Assert.NotEmpty(publishResult.MessageId);

        var received = await queue.ReceiveAsync(maxMessages: 10, waitTime: TimeSpan.FromSeconds(5));
        var single = Assert.Single(received);
        Assert.Equal(message.OrderId, single.Body.OrderId);
        Assert.Equal(message.Total, single.Body.Total);
        Assert.Equal(1, single.DeliveryCount);
        Assert.Equal("test", single.Attributes["origin"]);

        await queue.DeleteAsync(single);
        // Zero wait: the default is a 20-second long poll, pointless against a known-empty queue.
        Assert.Empty(await queue.ReceiveAsync(maxMessages: 10, waitTime: TimeSpan.Zero));
    }

    [SqsFact]
    public async Task Publish_ViaPublisherFacade()
    {
        await using var provider = CreateProvider(builder => builder.AddQueue<TestOrder>("test-orders"));

        await provider.GetRequiredService<IMessagePublisher>().PublishAsync(new TestOrder { OrderId = Guid.NewGuid() });

        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();
        Assert.Single(await queue.ReceiveAsync(maxMessages: 10, waitTime: TimeSpan.FromSeconds(5)));
    }

    [SqsFact]
    public async Task Abandon_ReturnsMessageWithIncrementedDeliveryCount()
    {
        await using var provider = CreateProvider(builder => builder.AddQueue<TestOrder>("test-orders"));
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        await queue.PublishAsync(new TestOrder { OrderId = Guid.NewGuid() });

        var first = Assert.Single(await queue.ReceiveAsync(waitTime: TimeSpan.FromSeconds(5)));
        await queue.AbandonAsync(first);

        var second = Assert.Single(await queue.ReceiveAsync(waitTime: TimeSpan.FromSeconds(5)));
        Assert.Equal(2, second.DeliveryCount);
    }

    [SqsFact]
    public async Task ExistsAndCount()
    {
        await using var provider = CreateProvider(builder => builder.AddQueue<TestOrder>("test-orders"));
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        // The queue is created lazily on first use, so before any publish it does not exist yet.
        Assert.False(await queue.ExistsAsync());

        await queue.PublishAsync(new TestOrder { OrderId = Guid.NewGuid() });

        Assert.True(await queue.ExistsAsync());
        Assert.Equal(1, await queue.GetApproximateCountAsync());

        await queue.PurgeAsync();
        Assert.Equal(0, await queue.GetApproximateCountAsync());
    }

    [SqsFact]
    public async Task Fifo_WithoutContentDedup_ParameterlessPublishWorks()
    {
        // Without content-based deduplication SQS requires a deduplication id on every message; the
        // library generates one, so a plain publish must succeed.
        await using var provider = CreateProvider(builder => builder.AddQueue<TestOrder>("test-orders-fd", settings => settings.Fifo = true));
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        await queue.PublishAsync(new TestOrder { Total = 1 });
        await queue.PublishAsync(new TestOrder { Total = 2 });

        var received = await queue.ReceiveAsync(maxMessages: 10, waitTime: TimeSpan.FromSeconds(5));
        Assert.Equal(2, received.Count);
    }

    [SqsFact]
    public async Task PoisonMessage_DeletePolicy_RemovesMessage()
    {
        // Two message types share one physical queue: a SourceEvent received through the TargetEvent
        // queue is poison (type attribute mismatch), and the Delete policy removes it for good.
        await using var provider = CreateProvider(builder => builder
            .AddQueue<SourceEvent>("poison-queue", settings => settings.VisibilityTimeout = TimeSpan.FromSeconds(1))
            .AddQueue<TargetEvent>("poison-queue", settings => settings.PoisonMessageHandling = PoisonMessageHandling.Delete));

        var sourceQueue = provider.GetRequiredService<IMessageQueue<SourceEvent>>();
        var targetQueue = provider.GetRequiredService<IMessageQueue<TargetEvent>>();

        await sourceQueue.PublishAsync(new SourceEvent { Value = "poison" });

        Assert.Empty(await targetQueue.ReceiveAsync(maxMessages: 10, waitTime: TimeSpan.FromSeconds(2)));

        // Deleted, not just invisible: after the 1-second visibility timeout it would have reappeared.
        await Task.Delay(1500);
        Assert.Empty(await sourceQueue.ReceiveAsync(maxMessages: 10, waitTime: TimeSpan.FromSeconds(2)));
    }

    [SqsFact]
    public async Task PoisonMessage_SerializerThrowsAnyException_BatchSurvives()
    {
        // A serializer is an extension point and may throw anything; one bad payload must not take the
        // rest of the batch down with it.
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton<IMessageSerializer>(new PickySerializer());
        services.AddSqsMessaging(options =>
        {
            SqsEnvironment.Apply(options);
            options.QueueNameSuffix = "-" + Guid.NewGuid().ToString("N")[..8];
        })
        .AddQueue<TestOrder>("picky-orders", settings => settings.PoisonMessageHandling = PoisonMessageHandling.Delete);

        await using var provider = services.BuildServiceProvider();
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        await queue.PublishAsync(new TestOrder { Total = PickySerializer.PoisonTotal });
        await queue.PublishAsync(new TestOrder { Total = 7 });

        var received = new List<ReceivedMessage<TestOrder>>();
        while (received.Count < 1)
            received.AddRange(await queue.ReceiveAsync(maxMessages: 10, waitTime: TimeSpan.FromSeconds(5)));

        var good = Assert.Single(received);
        Assert.Equal(7, good.Body.Total);
        await queue.DeleteAsync(good);

        // The poison message was deleted, not merely skipped.
        Assert.Empty(await queue.ReceiveAsync(maxMessages: 10, waitTime: TimeSpan.FromSeconds(3)));
    }

    /// <summary>Throws a plain FormatException — not a MessagingException — for one specific payload.</summary>
    class PickySerializer : IMessageSerializer
    {
        public const decimal PoisonTotal = 666m;

        readonly JsonMessageSerializer inner = new();

        public string GetTypeName(Type messageType) => inner.GetTypeName(messageType);
        public string Serialize(object message) => inner.Serialize(message);
        public byte[] SerializeToUtf8Bytes(object message) => inner.SerializeToUtf8Bytes(message);

        public object Deserialize(string payload, Type messageType)
        {
            var message = inner.Deserialize(payload, messageType);
            if (message is TestOrder { Total: PoisonTotal })
                throw new FormatException("unsupported payload shape");

            return message;
        }
    }

    [SqsFact]
    public async Task Context_EnsureQueues_CreatesAndRoundtrips()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSqsMessaging<TestMessaging>(options =>
        {
            SqsEnvironment.Apply(options);
            options.AutoCreateQueues = false; // provisioning goes through EnsureQueuesAsync here
            options.QueueNameSuffix = "-" + Guid.NewGuid().ToString("N")[..8];
        })
        .ConfigureQueue<ContextOrder>(settings => settings.MaxReceiveCount = 3);

        await using var provider = services.BuildServiceProvider();
        var context = provider.GetRequiredService<TestMessaging>();

        Assert.False(await context.Orders.ExistsAsync());

        await context.EnsureQueuesAsync();

        // Both the queue and its dead-letter twin exist now, before any publish.
        Assert.True(await context.Orders.ExistsAsync());

        await context.Orders.PublishAsync(new ContextOrder { OrderId = Guid.NewGuid() });
        var received = Assert.Single(await context.Orders.ReceiveAsync(maxMessages: 10, waitTime: TimeSpan.FromSeconds(5)));
        await context.Orders.DeleteAsync(received);
    }

    [SqsFact]
    public async Task Fifo_KeepsOrderWithinGroup()
    {
        await using var provider = CreateProvider(builder => builder.AddQueue<TestOrder>("test-orders-f", settings =>
        {
            settings.Fifo = true;
            settings.ContentBasedDeduplication = true;
        }));
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();
        Assert.EndsWith(".fifo", queue.Name);

        for (var i = 1; i <= 3; i++)
            await queue.PublishAsync(new TestOrder { Total = i }, new PublishOptions { GroupId = "g1" });

        var received = new List<ReceivedMessage<TestOrder>>();
        while (received.Count < 3)
        {
            var batch = await queue.ReceiveAsync(maxMessages: 10, waitTime: TimeSpan.FromSeconds(5));
            foreach (var message in batch)
            {
                received.Add(message);
                await queue.DeleteAsync(message);
            }
        }

        Assert.Equal([1m, 2m, 3m], received.Select(m => m.Body.Total));
    }

    public class TestOrder
    {
        public Guid OrderId { get; set; }
        public decimal Total { get; set; }
    }

    public class ContextOrder
    {
        public Guid OrderId { get; set; }
    }

    public class TestMessaging : MessagingContext
    {
        [Queue("ctx-orders")] public IMessageQueue<ContextOrder> Orders { get; private set; } = null!;
    }

    public class SourceEvent
    {
        public string? Value { get; set; }
    }

    public class TargetEvent
    {
        public string? Value { get; set; }
    }
}
