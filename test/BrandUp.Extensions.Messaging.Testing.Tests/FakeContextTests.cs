using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

public class FakeContextTests
{
    static ServiceProvider CreateProvider(out InMemoryMessageBus bus)
    {
        var services = new ServiceCollection();
        services.AddFakeMessaging<OrderMessaging>();

        var provider = services.BuildServiceProvider();
        bus = provider.GetRequiredService<InMemoryMessageBus>();
        return provider;
    }

    [Fact]
    public async Task Context_PublishThroughProperty_IsCaptured()
    {
        await using var provider = CreateProvider(out var bus);
        var context = provider.GetRequiredService<OrderMessaging>();

        Assert.Same(context.Created, provider.GetRequiredService<IMessageQueue<OrderCreated>>());
        Assert.Equal("order-created", context.Created.Name);

        await context.Created.PublishAsync(new OrderCreated { OrderId = Guid.NewGuid() });

        Assert.Single(bus.PublishedOf<OrderCreated>());
        Assert.Single(await context.Created.ReceiveAsync());
    }

    [Fact]
    public async Task Context_PublisherFacade_RoutesToContextQueue()
    {
        await using var provider = CreateProvider(out var bus);

        await provider.GetRequiredService<IMessagePublisher>().PublishAsync(new OrderCancelled());

        Assert.Equal("orders-cancelled", Assert.Single(bus.Published).QueueName);
    }

    [Fact]
    public async Task Context_EnsureQueues_IsNoOp()
    {
        await using var provider = CreateProvider(out _);

        // Fake queues always exist; the call must simply succeed.
        await provider.GetRequiredService<OrderMessaging>().EnsureQueuesAsync();
    }

    [Fact]
    public async Task Context_WithQueuesAndStreams_BindsBoth()
    {
        var services = new ServiceCollection();
        services.AddFakeMessaging<MixedMessaging>();

        await using var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<InMemoryMessageBus>();
        var context = provider.GetRequiredService<MixedMessaging>();

        // In production this context takes two registrations, one per transport; a test should not
        // have to know that.
        Assert.Same(context.Created, provider.GetRequiredService<IMessageQueue<OrderCreated>>());
        Assert.Same(context.Events, provider.GetRequiredService<IMessageStream<OrderEvent>>());
        Assert.Same(context.Events, context.Stream<OrderEvent>());
        Assert.Single(context.Queues);
        Assert.Single(context.Streams);
        Assert.Equal("order-events", context.Events.Name);

        await context.Created.PublishAsync(new OrderCreated { OrderId = Guid.NewGuid() });
        await context.Events.PublishAsync(new OrderEvent());

        Assert.Single(bus.PublishedOf<OrderCreated>());
        Assert.Single(bus.PublishedOf<OrderEvent>());

        // Only the queue is provisioned; the stream is left alone.
        await context.EnsureQueuesAsync();
    }

    [Queue("order-created")]
    public class OrderCreated
    {
        public Guid OrderId { get; set; }
    }

    public class OrderCancelled;

    [Queue("order-events")]
    public class OrderEvent;

    public class OrderMessaging : MessagingContext
    {
        public IMessageQueue<OrderCreated> Created { get; private set; } = null!;
        [Queue("orders-cancelled")] public IMessageQueue<OrderCancelled> Cancelled { get; private set; } = null!;
    }

    public class MixedMessaging : MessagingContext
    {
        public IMessageQueue<OrderCreated> Created { get; private set; } = null!;
        public IMessageStream<OrderEvent> Events { get; private set; } = null!;
    }
}
