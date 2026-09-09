using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Batch publishing as an application sees it: results in the order the messages were passed in,
/// per-message options honoured, and the same rules a single publish is held to.
/// </summary>
public class FakeBatchPublishTests
{
    static ServiceProvider CreateProvider(out InMemoryMessageBus bus)
    {
        var services = new ServiceCollection();
        services.AddFakeMessaging()
            .AddQueue<OrderCreated>()
            .AddStream<OrderEvent>();

        var provider = services.BuildServiceProvider();
        bus = provider.GetRequiredService<InMemoryMessageBus>();
        return provider;
    }

    [Fact]
    public async Task Queue_PublishesEveryMessage_InOrder()
    {
        await using var provider = CreateProvider(out var bus);
        var queue = provider.GetRequiredService<IMessageQueue<OrderCreated>>();

        var results = await queue.PublishAsync(
            [.. Enumerable.Range(1, 25).Select(i => new PublishMessage<OrderCreated>(new OrderCreated { Number = i }))]);

        Assert.Equal(25, results.Count);
        Assert.Equal([.. Enumerable.Range(1, 25)], bus.PublishedOf<OrderCreated>().Select(m => m.Number));
        // Every result is filled in, and they line up with the messages that were passed in.
        Assert.Equal([.. bus.Published.Select(m => m.MessageId)], results.Select(r => r.MessageId));
    }

    [Fact]
    public async Task Batch_CarriesPerMessageOptions()
    {
        await using var provider = CreateProvider(out var bus);
        var stream = provider.GetRequiredService<IMessageStream<OrderEvent>>();

        await stream.PublishAsync(
        [
            new(new OrderEvent { Number = 1 }, new PublishOptions { GroupId = "order-1" }),
            new(new OrderEvent { Number = 2 }, new PublishOptions { GroupId = "order-2" }),
        ]);

        // A batch is rarely uniform: on a stream each record usually carries its own partition key.
        Assert.Equal(["order-1", "order-2"], bus.Published.Select(m => m.GroupId));
    }

    [Fact]
    public async Task Batch_WithSharedOptions_IsTheShorthand()
    {
        await using var provider = CreateProvider(out var bus);
        var stream = provider.GetRequiredService<IMessageStream<OrderEvent>>();

        await stream.PublishAsync(
            [new OrderEvent { Number = 1 }, new OrderEvent { Number = 2 }],
            new PublishOptions { GroupId = "order-1" });

        Assert.Equal(["order-1", "order-1"], bus.Published.Select(m => m.GroupId));
    }

    [Fact]
    public async Task Batch_IsCheckedLikeASinglePublish()
    {
        await using var provider = CreateProvider(out var bus);
        var stream = provider.GetRequiredService<IMessageStream<OrderEvent>>();

        // The second message is the invalid one, and nothing is published: the batch is prepared before
        // the first message goes out.
        await Assert.ThrowsAsync<ArgumentException>(() => stream.PublishAsync(
        [
            new(new OrderEvent { Number = 1 }),
            new(new OrderEvent { Number = 2 }, new PublishOptions { Delay = TimeSpan.FromSeconds(1) }),
        ]));

        Assert.Empty(bus.Published);
    }

    [Fact]
    public async Task EmptyBatch_PublishesNothing()
    {
        await using var provider = CreateProvider(out var bus);

        Assert.Empty(await provider.GetRequiredService<IMessageQueue<OrderCreated>>().PublishAsync([]));
        Assert.Empty(bus.Published);
    }

    [Fact]
    public async Task Publisher_RoutesABatchByMessageType()
    {
        await using var provider = CreateProvider(out var bus);
        var publisher = provider.GetRequiredService<IMessagePublisher>();

        await publisher.PublishAsync([new OrderCreated { Number = 1 }, new OrderCreated { Number = 2 }]);
        await publisher.PublishAsync<OrderEvent>([new(new OrderEvent { Number = 3 })]);

        Assert.Equal(2, bus.PublishedOf<OrderCreated>().Count);
        Assert.Single(bus.PublishedOf<OrderEvent>());
    }

    [Fact]
    public async Task Batch_DispatchesToHandlersLikeSinglePublishes()
    {
        var services = new ServiceCollection();
        services.AddSingleton<List<int>>();
        services.AddFakeMessaging()
            .AddQueue<OrderCreated>()
            .AddHandler<OrderCreated, OrderCreatedHandler>();

        await using var provider = services.BuildServiceProvider();
        var bus = provider.GetRequiredService<InMemoryMessageBus>();

        await provider.GetRequiredService<IMessageQueue<OrderCreated>>()
            .PublishAsync([new OrderCreated { Number = 1 }, new OrderCreated { Number = 2 }]);

        Assert.Equal(2, await bus.DispatchPendingAsync(provider));
        Assert.Equal([1, 2], provider.GetRequiredService<List<int>>());
    }

    [Queue("order-created")]
    public class OrderCreated
    {
        public int Number { get; set; }
    }

    [Queue("order-events")]
    public class OrderEvent
    {
        public int Number { get; set; }
    }

    public class OrderCreatedHandler(List<int> handled) : IMessageHandler<OrderCreated>
    {
        public Task HandleAsync(MessageContext<OrderCreated> context, CancellationToken cancellationToken)
        {
            handled.Add(context.Message.Number);
            return Task.CompletedTask;
        }
    }
}
