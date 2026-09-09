using Amazon.Kinesis;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

internal interface IKinesisClientFactory
{
    /// <summary>Client of a connection; one client per connection name, created on first use.</summary>
    IAmazonKinesis Get(string connectionName);
}

internal sealed class KinesisClientFactory(
    IOptionsMonitor<KinesisMessagingOptions> options,
    IServiceProvider services,
    IEnumerable<MessagingCredentialsRegistration> credentials) : IKinesisClientFactory, IDisposable
{
    // Lazy, because GetOrAdd may run its factory more than once under contention and a discarded
    // AmazonKinesisClient would leak its connection pool.
    readonly System.Collections.Concurrent.ConcurrentDictionary<string, Lazy<IAmazonKinesis>> clients = new(StringComparer.Ordinal);

    public IAmazonKinesis Get(string connectionName)
        => clients.GetOrAdd(
            connectionName,
            name => new Lazy<IAmazonKinesis>(
                () => Create(options.Get(name), CredentialsRegistrations.Resolve(services, credentials, name)),
                LazyThreadSafetyMode.ExecutionAndPublication)).Value;

    static IAmazonKinesis Create(KinesisMessagingOptions options, IMessagingCredentialsProvider? provider)
    {
        var config = new AmazonKinesisConfig();
        AwsConnection.Configure(config, options);

        // No provider and no static keys -> construct without credentials, so the SDK default chain applies.
        return AwsConnection.CreateCredentials(options, provider) is { } credentials
            ? new AmazonKinesisClient(credentials, config)
            : new AmazonKinesisClient(config);
    }

    public void Dispose()
    {
        foreach (var client in clients.Values)
        {
            if (client.IsValueCreated)
                client.Value.Dispose();
        }

        clients.Clear();
    }
}
