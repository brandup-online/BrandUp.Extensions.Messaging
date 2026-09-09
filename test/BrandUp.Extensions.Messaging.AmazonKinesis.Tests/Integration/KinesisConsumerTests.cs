using System.Collections.Concurrent;
using Amazon;
using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Runtime;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace BrandUp.Extensions.Messaging.Integration;

/// <summary>
/// End-to-end reader tests against a Kinesis-compatible server (LocalStack in CI): checkpointing,
/// resuming after a restart, and per-shard ordering.
/// </summary>
[Trait("Category", "Integration")]
public class KinesisConsumerTests : IAsyncLifetime
{
    string streamName = "";
    IAmazonKinesis client = null!;

    public async ValueTask InitializeAsync()
    {
        if (!KinesisEnvironment.IsConfigured)
            return;

        var config = new AmazonKinesisConfig
        {
            ServiceURL = KinesisEnvironment.ServiceUrl,
            AuthenticationRegion = KinesisEnvironment.Region,
        };
        client = new AmazonKinesisClient(
            new BasicAWSCredentials(KinesisEnvironment.AccessKey ?? "test", KinesisEnvironment.SecretKey ?? "test"), config);

        streamName = "test-stream-" + Guid.NewGuid().ToString("N")[..8];
        await client.CreateStreamAsync(new CreateStreamRequest { StreamName = streamName, ShardCount = 1 });

        // The stream is not readable until it goes ACTIVE.
        for (var i = 0; i < 60; i++)
        {
            var description = await client.DescribeStreamSummaryAsync(new DescribeStreamSummaryRequest { StreamName = streamName });
            if (description.StreamDescriptionSummary.StreamStatus == StreamStatus.ACTIVE)
                return;

            await Task.Delay(500);
        }

        throw new InvalidOperationException($"Stream {streamName} did not become active.");
    }

    public async ValueTask DisposeAsync()
    {
        if (client is not null)
        {
            await client.DeleteStreamAsync(new DeleteStreamRequest { StreamName = streamName });
            client.Dispose();
        }
    }

    IHost BuildReader(
        Handled handled,
        InMemoryCheckpointStore checkpoints,
        Action<KinesisConsumerOptions>? configure = null,
        InMemoryShardLeaseStore? leases = null)
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(handled);
        builder.Services.AddSingleton<ICheckpointStore>(checkpoints);
        if (leases is not null)
            builder.Services.AddSingleton<IShardLeaseStore>(leases);
        builder.Services.AddKinesisMessaging(options =>
        {
            KinesisEnvironment.Apply(options);
            options.Streams["orders"] = streamName;
        })
        .AddStream<OrderEvent>("orders")
        .AddConsumer<OrderEvent, OrderEventHandler>(options =>
        {
            options.StartPosition = StreamStartPosition.Oldest;
            options.EmptyReadDelay = TimeSpan.FromMilliseconds(200);
            options.ShardDiscoveryInterval = TimeSpan.FromSeconds(2);
            configure?.Invoke(options);
        });

        return builder.Build();
    }

    async Task PublishAsync(IHost host, params int[] numbers)
    {
        var publisher = host.Services.GetRequiredService<IMessagePublisher>();
        foreach (var number in numbers)
            await publisher.PublishAsync(new OrderEvent { Number = number }, new PublishOptions { GroupId = "same-shard" });
    }

    static async Task WaitForAsync(Func<bool> condition, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        while (!condition())
            await Task.Delay(100, cts.Token);
    }

    [KinesisFact]
    public async Task Reader_HandlesRecords_InOrder_AndCheckpoints()
    {
        var handled = new Handled();
        var checkpoints = new InMemoryCheckpointStore();

        using var host = BuildReader(handled, checkpoints);
        await host.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            await PublishAsync(host, 1, 2, 3);
            await WaitForAsync(() => handled.Numbers.Count >= 3, TimeSpan.FromSeconds(60));

            // One shard, one partition key: order is the contract.
            Assert.Equal([1, 2, 3], handled.Numbers);

            // The position was committed for the group.
            var checkpoint = Assert.Single(checkpoints.Checkpoints);
            Assert.Equal(streamName, checkpoint.Key.StreamName);
            Assert.Equal("default", checkpoint.Key.ConsumerGroup);
            Assert.NotEmpty(checkpoint.Value.Position);
            Assert.False(checkpoint.Value.Completed);
        }
        finally
        {
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [KinesisFact]
    public async Task Reader_ResumesFromCheckpoint_AfterRestart()
    {
        var checkpoints = new InMemoryCheckpointStore();

        var first = new Handled();
        using (var host = BuildReader(first, checkpoints))
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            await PublishAsync(host, 1, 2);
            await WaitForAsync(() => first.Numbers.Count >= 2, TimeSpan.FromSeconds(60));
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        // A new reader with the same checkpoints must not see what the first one already handled.
        var second = new Handled();
        using (var host = BuildReader(second, checkpoints))
        {
            await host.StartAsync(TestContext.Current.CancellationToken);
            await PublishAsync(host, 3);
            await WaitForAsync(() => second.Numbers.Count >= 1, TimeSpan.FromSeconds(60));
            await host.StopAsync(TestContext.Current.CancellationToken);
        }

        Assert.Equal([1, 2], first.Numbers);
        Assert.Equal([3], second.Numbers);
    }

    [KinesisFact]
    public async Task Reader_SeparateConsumerGroups_EachSeeEveryRecord()
    {
        var checkpoints = new InMemoryCheckpointStore();
        var billing = new Handled();
        var analytics = new Handled();

        using var billingHost = BuildReader(billing, checkpoints, options => options.ConsumerGroup = "billing");
        using var analyticsHost = BuildReader(analytics, checkpoints, options => options.ConsumerGroup = "analytics");

        await billingHost.StartAsync(TestContext.Current.CancellationToken);
        await analyticsHost.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            await PublishAsync(billingHost, 1, 2);

            await WaitForAsync(() => billing.Numbers.Count >= 2 && analytics.Numbers.Count >= 2, TimeSpan.FromSeconds(60));

            Assert.Equal([1, 2], billing.Numbers);
            Assert.Equal([1, 2], analytics.Numbers);
            Assert.Equal(2, checkpoints.Checkpoints.Count);   // one per group
        }
        finally
        {
            await billingHost.StopAsync(TestContext.Current.CancellationToken);
            await analyticsHost.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [KinesisFact]
    public async Task Reader_FailingHandler_RetriesThenSkips_AndKeepsGoing()
    {
        var handled = new Handled { FailOn = 2 };
        var checkpoints = new InMemoryCheckpointStore();

        using var host = BuildReader(handled, checkpoints, options =>
        {
            options.MaxDeliveryAttempts = 2;
            options.RetryDelay = TimeSpan.FromMilliseconds(200);
        });

        await host.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            await PublishAsync(host, 1, 2, 3);

            // 2 is retried twice, then skipped; 3 must still arrive - a poison record cannot stall the shard.
            await WaitForAsync(() => handled.Numbers.Contains(3), TimeSpan.FromSeconds(60));

            Assert.Equal([1, 3], handled.Numbers);
            Assert.Equal(2, handled.Attempts[2]);
        }
        finally
        {
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    [KinesisFact]
    public async Task Readers_OfOneGroup_LeaseTheShard_AndTakeOverWhenOneStops()
    {
        var checkpoints = new InMemoryCheckpointStore();
        var leases = new InMemoryShardLeaseStore();
        var first = new Handled();
        var second = new Handled();

        void ShortLeases(KinesisConsumerOptions options)
        {
            options.LeaseDuration = TimeSpan.FromSeconds(10);
            options.LeaseRenewInterval = TimeSpan.FromSeconds(2);
        }

        using var firstHost = BuildReader(first, checkpoints, ShortLeases, leases);
        using var secondHost = BuildReader(second, checkpoints, ShortLeases, leases);

        await firstHost.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            await WaitForAsync(() => leases.Leases.Count == 1, TimeSpan.FromSeconds(60));

            // The second reader of the group finds the only shard leased and reads nothing, rather than
            // delivering every record a second time.
            await secondHost.StartAsync(TestContext.Current.CancellationToken);
            await PublishAsync(firstHost, 1, 2);
            await WaitForAsync(() => first.Numbers.Count >= 2, TimeSpan.FromSeconds(60));

            Assert.Equal([1, 2], first.Numbers);
            Assert.Empty(second.Numbers);
            Assert.Single(leases.Leases);
        }
        finally
        {
            // Stopping releases the lease, so the shard is free at once instead of after it expires.
            await firstHost.StopAsync(TestContext.Current.CancellationToken);
        }

        try
        {
            await WaitForAsync(() => leases.Leases.Count == 1, TimeSpan.FromSeconds(60));
            await PublishAsync(secondHost, 3);
            await WaitForAsync(() => second.Numbers.Count >= 1, TimeSpan.FromSeconds(60));

            // Taken over at the checkpoint the first reader left: what it handled is not handled again.
            Assert.Equal([3], second.Numbers);
        }
        finally
        {
            await secondHost.StopAsync(TestContext.Current.CancellationToken);
        }

        // Every lease is given back when the readers stop.
        Assert.Empty(leases.Leases);
    }

    [KinesisFact]
    public async Task Reader_HandlesABatchPublishedAtOnce_InOrder()
    {
        var handled = new Handled();
        var checkpoints = new InMemoryCheckpointStore();

        using var host = BuildReader(handled, checkpoints);
        await host.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            var results = await host.Services.GetRequiredService<IMessageStream<OrderEvent>>().PublishAsync(
                [.. Enumerable.Range(1, 5).Select(i => new PublishMessage<OrderEvent>(
                    new OrderEvent { Number = i }, new PublishOptions { GroupId = "same-shard" }))]);

            Assert.Equal(5, results.Count);
            Assert.All(results, result => Assert.NotEmpty(result.SequenceNumber!));

            await WaitForAsync(() => handled.Numbers.Count >= 5, TimeSpan.FromSeconds(60));

            // One partition key, one shard: PutRecords keeps the order of the batch.
            Assert.Equal([1, 2, 3, 4, 5], handled.Numbers);
        }
        finally
        {
            await host.StopAsync(TestContext.Current.CancellationToken);
        }
    }

    public class OrderEvent
    {
        public int Number { get; set; }
    }

    public class Handled
    {
        readonly List<int> numbers = [];

        public int? FailOn { get; init; }
        public ConcurrentDictionary<int, int> Attempts { get; } = new();

        public IReadOnlyList<int> Numbers
        {
            get { lock (numbers) return [.. numbers]; }
        }

        public void Add(int number)
        {
            lock (numbers)
                numbers.Add(number);
        }
    }

    public class OrderEventHandler(Handled handled) : IMessageHandler<OrderEvent>
    {
        public Task HandleAsync(MessageContext<OrderEvent> context, CancellationToken cancellationToken)
        {
            var number = context.Message.Number;
            handled.Attempts.AddOrUpdate(number, 1, (_, count) => count + 1);

            if (handled.FailOn == number)
                throw new InvalidOperationException($"handler failed for {number}");

            handled.Add(number);
            return Task.CompletedTask;
        }
    }
}
