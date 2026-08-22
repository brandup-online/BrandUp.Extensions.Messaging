using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

public class FakeMessagingTests
{
    static ServiceProvider CreateProvider(out InMemoryMessageBus bus, Action<FakeMessagingBuilder>? register = null)
    {
        var services = new ServiceCollection();
        var builder = services.AddFakeMessaging().AddQueue<TestOrder>("test-orders");
        register?.Invoke(builder);

        var provider = services.BuildServiceProvider();
        bus = provider.GetRequiredService<InMemoryMessageBus>();
        return provider;
    }

    [Fact]
    public async Task Publish_IsCaptured()
    {
        await using var provider = CreateProvider(out var bus);
        var publisher = provider.GetRequiredService<IMessagePublisher>();

        var message = new TestOrder { OrderId = Guid.NewGuid() };
        var result = await publisher.PublishAsync(message, new PublishOptions { GroupId = "g1" });

        var captured = Assert.Single(bus.Published);
        Assert.Equal(result.MessageId, captured.MessageId);
        Assert.Equal("g1", captured.Options?.GroupId);

        // Captured as a payload and read back, exactly like a real transport: equal by value, and
        // deliberately not the publisher's instance.
        var published = Assert.Single(bus.PublishedOf<TestOrder>());
        Assert.Equal(message.OrderId, published.OrderId);
        Assert.NotSame(message, published);
    }

    [Fact]
    public async Task Publish_RoundTripsThroughSerializer()
    {
        await using var provider = CreateProvider(out var bus);
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        var message = new TestOrder { OrderId = Guid.NewGuid() };
        await queue.PublishAsync(message);

        // Mutating after publish must not change what a consumer sees.
        message.OrderId = Guid.NewGuid();

        var received = Assert.Single(await queue.ReceiveAsync());
        Assert.NotEqual(message.OrderId, received.Body.OrderId);
    }

    [Fact]
    public async Task Publish_UnserializableMessage_ThrowsLikeProduction()
    {
        var services = new ServiceCollection();
        services.AddFakeMessaging().AddQueue<Unserializable>("unserializable");
        await using var provider = services.BuildServiceProvider();

        await Assert.ThrowsAnyAsync<Exception>(
            () => provider.GetRequiredService<IMessageQueue<Unserializable>>().PublishAsync(new Unserializable()));
    }

    [Fact]
    public async Task Publish_EnforcesSqsPublishLimits()
    {
        await using var provider = CreateProvider(out _);
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => queue.PublishAsync(new TestOrder(), new PublishOptions { Delay = TimeSpan.FromHours(1) }));

        // The reserved type attribute cannot be set by the publisher.
        await Assert.ThrowsAsync<ArgumentException>(
            () => queue.PublishAsync(new TestOrder(), new PublishOptions { Attributes = { ["BrandUp-MessageType"] = "X" } }));
    }

    [Fact]
    public async Task Publish_FifoQueue_RejectsPerMessageDelay()
    {
        var services = new ServiceCollection();
        services.AddFakeMessaging().AddQueue<TestOrder>("fifo-orders", settings => settings.Fifo = true);
        await using var provider = services.BuildServiceProvider();

        await Assert.ThrowsAsync<ArgumentException>(() => provider.GetRequiredService<IMessageQueue<TestOrder>>()
            .PublishAsync(new TestOrder(), new PublishOptions { Delay = TimeSpan.FromMinutes(1) }));
    }

    [Fact]
    public async Task Publish_FifoQueue_SetsGroupIdOnDelivery()
    {
        var services = new ServiceCollection();
        services.AddFakeMessaging().AddQueue<TestOrder>("fifo-orders", settings => settings.Fifo = true);
        await using var provider = services.BuildServiceProvider();
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        await queue.PublishAsync(new TestOrder(), new PublishOptions { GroupId = "g1" });

        Assert.Equal("g1", Assert.Single(await queue.ReceiveAsync()).GroupId);
    }

    [Fact]
    public async Task ReceiveDelete_Roundtrip()
    {
        await using var provider = CreateProvider(out _);
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        await queue.PublishAsync(new TestOrder { OrderId = Guid.NewGuid() });
        Assert.Equal(1, await queue.GetApproximateCountAsync());

        var received = Assert.Single(await queue.ReceiveAsync(maxMessages: 10));
        Assert.Equal(0, await queue.GetApproximateCountAsync());

        await queue.DeleteAsync(received);
        Assert.Empty(await queue.ReceiveAsync(maxMessages: 10));

        // A completed receipt handle cannot be reused.
        await Assert.ThrowsAsync<MessagingException>(() => queue.DeleteAsync(received));
    }

    [Fact]
    public async Task Receive_DeliversPublishAttributes()
    {
        await using var provider = CreateProvider(out _);
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        await queue.PublishAsync(new TestOrder(), new PublishOptions { Attributes = { ["tenant"] = "acme" } });

        var received = Assert.Single(await queue.ReceiveAsync());
        Assert.Equal("acme", received.Attributes["tenant"]);
    }

    [Fact]
    public async Task Receive_EnforcesSqsCaps()
    {
        await using var provider = CreateProvider(out _);
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        // Same limits as the real SQS queue, so a loop that passes here does not crash in production.
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => queue.ReceiveAsync(maxMessages: 50));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => queue.ReceiveAsync(waitTime: TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public async Task Abandon_ReturnsMessageFirstWithIncrementedDeliveryCount()
    {
        await using var provider = CreateProvider(out _);
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        await queue.PublishAsync(new TestOrder());
        var first = Assert.Single(await queue.ReceiveAsync());

        await queue.AbandonAsync(first);

        var second = Assert.Single(await queue.ReceiveAsync());
        Assert.Equal(2, second.DeliveryCount);
    }

    [Fact]
    public async Task DispatchPending_DeliversToHandler()
    {
        RecordingHandler.Handled.Clear();
        await using var provider = CreateProvider(out var bus, builder => builder.AddHandler<TestOrder, RecordingHandler>());
        var publisher = provider.GetRequiredService<IMessagePublisher>();

        await publisher.PublishAsync(new TestOrder { OrderId = Guid.NewGuid() });
        await publisher.PublishAsync(new TestOrder { OrderId = Guid.NewGuid() });

        var dispatched = await bus.DispatchPendingAsync(provider);

        Assert.Equal(2, dispatched);
        Assert.Equal(2, RecordingHandler.Handled.Count);
        Assert.Equal(0, await provider.GetRequiredService<IMessageQueue<TestOrder>>().GetApproximateCountAsync());
    }

    [Fact]
    public async Task DispatchPending_HandlerSeesPublishAttributes()
    {
        RecordingHandler.Handled.Clear();
        await using var provider = CreateProvider(out var bus, builder => builder.AddHandler<TestOrder, RecordingHandler>());

        await provider.GetRequiredService<IMessagePublisher>()
            .PublishAsync(new TestOrder(), new PublishOptions { Attributes = { ["tenant"] = "acme" } });
        await bus.DispatchPendingAsync(provider);

        var context = Assert.Single(RecordingHandler.Handled);
        Assert.Equal("acme", context.Attributes["tenant"]);
        Assert.Equal("test-orders", context.QueueName);
    }

    [Fact]
    public async Task DispatchPending_HandlerThrows_MessageStaysPending()
    {
        await using var provider = CreateProvider(out var bus, builder => builder.AddHandler<TestOrder, ThrowingHandler>());
        var queue = provider.GetRequiredService<IMessageQueue<TestOrder>>();

        await queue.PublishAsync(new TestOrder());

        await Assert.ThrowsAsync<InvalidOperationException>(() => bus.DispatchPendingAsync(provider));

        var pending = Assert.Single(await queue.ReceiveAsync());
        Assert.Equal(2, pending.DeliveryCount);
    }

    [Fact]
    public async Task AddFakeMessaging_RepeatedCalls_ShareOneBus()
    {
        var services = new ServiceCollection();
        // Two composition modules, each registering its own queues.
        services.AddFakeMessaging().AddQueue<TestOrder>("test-orders");
        services.AddFakeMessaging().AddQueue<OtherMessage>("other");

        await using var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<InMemoryMessageBus>();

        await provider.GetRequiredService<IMessageQueue<TestOrder>>().PublishAsync(new TestOrder());
        await provider.GetRequiredService<IMessageQueue<OtherMessage>>().PublishAsync(new OtherMessage());

        Assert.Equal(2, bus.Published.Count);
    }

    [Fact]
    public void AddQueue_WithoutNameOrAttribute_Throws_LikeProduction()
    {
        var services = new ServiceCollection();
        var builder = services.AddFakeMessaging();

        Assert.Throws<ArgumentException>(() => builder.AddQueue<TestOrder>());
    }

    [Fact]
    public void AddQueue_SameTypeTwice_Throws_LikeProduction()
    {
        var services = new ServiceCollection();
        var builder = services.AddFakeMessaging().AddQueue<TestOrder>("test-orders");

        Assert.Throws<InvalidOperationException>(() => builder.AddQueue<TestOrder>("other"));
    }

    public class TestOrder
    {
        public Guid OrderId { get; set; }
    }

    public class OtherMessage;

    /// <summary>System.Text.Json cannot serialize a Type-valued property.</summary>
    public class Unserializable
    {
        public Type Value { get; set; } = typeof(string);
    }

    public class RecordingHandler : IMessageHandler<TestOrder>
    {
        public static List<MessageContext<TestOrder>> Handled { get; } = [];

        public Task HandleAsync(MessageContext<TestOrder> context, CancellationToken cancellationToken)
        {
            Handled.Add(context);
            return Task.CompletedTask;
        }
    }

    public class ThrowingHandler : IMessageHandler<TestOrder>
    {
        public Task HandleAsync(MessageContext<TestOrder> context, CancellationToken cancellationToken)
            => throw new InvalidOperationException("handler failed");
    }
}
