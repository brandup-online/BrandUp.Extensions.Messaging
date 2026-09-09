using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// The fake stream against the rules of a real one: what it captures, what it refuses, and that a
/// record reaches its handler the way the hosted reader would deliver it.
/// </summary>
public class FakeStreamTests
{
    static ServiceProvider CreateProvider(out InMemoryMessageBus bus)
    {
        var services = new ServiceCollection();
        services.AddFakeMessaging()
            .AddStream<OrderEvent>()
            .AddHandler<OrderEvent, OrderEventHandler>();
        services.AddSingleton<Handled>();

        var provider = services.BuildServiceProvider();
        bus = provider.GetRequiredService<InMemoryMessageBus>();
        return provider;
    }

    [Fact]
    public async Task Publish_IsCaptured_WithTheStreamName()
    {
        await using var provider = CreateProvider(out var bus);
        var stream = provider.GetRequiredService<IMessageStream<OrderEvent>>();

        Assert.Equal("order-events", stream.Name);

        var result = await stream.PublishAsync(new OrderEvent { Number = 1 });

        var published = Assert.Single(bus.Published);
        Assert.Equal("order-events", published.QueueName);
        Assert.Equal(result.MessageId, published.MessageId);
        // A stream answers with a sequence number, and uses it as the message id.
        Assert.Equal(result.MessageId, result.SequenceNumber);
        Assert.Equal(1, Assert.Single(bus.PublishedOf<OrderEvent>()).Number);
    }

    [Fact]
    public async Task Publish_GroupId_BecomesThePartitionKey()
    {
        await using var provider = CreateProvider(out var bus);
        var stream = provider.GetRequiredService<IMessageStream<OrderEvent>>();

        await stream.PublishAsync(new OrderEvent { Number = 1 }, new PublishOptions { GroupId = "order-42" });
        Assert.Equal("order-42", Assert.Single(bus.Published).GroupId);

        // Without a group a record still lands somewhere: the real transport generates a key.
        bus.Clear();
        await stream.PublishAsync(new OrderEvent { Number = 2 });
        Assert.NotNull(Assert.Single(bus.Published).GroupId);
    }

    [Fact]
    public async Task Publish_OptionsAStreamCannotHonour_AreRefused()
    {
        await using var provider = CreateProvider(out _);
        var stream = provider.GetRequiredService<IMessageStream<OrderEvent>>();

        // Exactly what the Kinesis stream refuses — the fake shares the rule, so a test fails where
        // production would.
        await Assert.ThrowsAsync<ArgumentException>(
            () => stream.PublishAsync(new OrderEvent(), new PublishOptions { Delay = TimeSpan.FromSeconds(1) }));
        await Assert.ThrowsAsync<ArgumentException>(
            () => stream.PublishAsync(new OrderEvent(), new PublishOptions { DeduplicationId = "x" }));
        await Assert.ThrowsAsync<ArgumentException>(
            () => stream.PublishAsync(new OrderEvent(), new PublishOptions { GroupId = new string('k', 257) }));
    }

    [Fact]
    public async Task DispatchPending_DeliversRecordsToTheHandler()
    {
        await using var provider = CreateProvider(out var bus);
        var stream = provider.GetRequiredService<IMessageStream<OrderEvent>>();

        await stream.PublishAsync(new OrderEvent { Number = 1 }, new PublishOptions { GroupId = "order-42" });
        await stream.PublishAsync(new OrderEvent { Number = 2 }, new PublishOptions { GroupId = "order-42" });

        Assert.Equal(2, await bus.DispatchPendingAsync(provider));

        var handled = provider.GetRequiredService<Handled>();
        Assert.Equal([1, 2], handled.Numbers);
        Assert.Equal("order-events", handled.StreamName);
        Assert.Equal("order-42", handled.GroupId);
    }

    [Fact]
    public async Task Publisher_RoutesToTheStream()
    {
        await using var provider = CreateProvider(out var bus);

        await provider.GetRequiredService<IMessagePublisher>().PublishAsync(new OrderEvent { Number = 7 });

        Assert.Equal("order-events", Assert.Single(bus.Published).QueueName);
    }

    [Fact]
    public void AddStream_BindingTheSameTypeTwice_Throws()
    {
        var services = new ServiceCollection();
        var builder = services.AddFakeMessaging().AddStream<OrderEvent>();

        // One message type, one destination — including across kinds, as in production.
        Assert.Throws<InvalidOperationException>(() => builder.AddStream<OrderEvent>());
        Assert.Throws<InvalidOperationException>(() => builder.AddQueue<OrderEvent>());
    }

    [Queue("order-events")]
    public class OrderEvent
    {
        public int Number { get; set; }
    }

    public class Handled
    {
        public List<int> Numbers { get; } = [];
        public string? StreamName { get; set; }
        public string? GroupId { get; set; }
    }

    public class OrderEventHandler(Handled handled) : IMessageHandler<OrderEvent>
    {
        public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken)
        {
            handled.Numbers.Add(context.Message.Number);
            handled.StreamName = context.QueueName;
            handled.GroupId = context.GroupId;

            return Task.CompletedTask;
        }
    }
}
