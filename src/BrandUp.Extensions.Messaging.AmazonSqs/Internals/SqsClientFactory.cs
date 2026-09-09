using Amazon.SQS;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

internal interface ISqsClientFactory
{
    /// <summary>Client of a connection; one client per connection name, created on first use.</summary>
    IAmazonSQS Get(string connectionName);
}

internal sealed class SqsClientFactory(
    IOptionsMonitor<SqsMessagingOptions> options,
    IServiceProvider services,
    IEnumerable<MessagingCredentialsRegistration> credentials) : ISqsClientFactory, IDisposable
{
    // Lazy, because GetOrAdd may run its factory more than once under contention and a discarded
    // AmazonSQSClient would leak its connection pool.
    readonly System.Collections.Concurrent.ConcurrentDictionary<string, Lazy<IAmazonSQS>> clients = new(StringComparer.Ordinal);

    public IAmazonSQS Get(string connectionName)
        => clients.GetOrAdd(
            connectionName,
            name => new Lazy<IAmazonSQS>(
                () => Create(options.Get(name), CredentialsRegistrations.Resolve(services, credentials, name)),
                LazyThreadSafetyMode.ExecutionAndPublication)).Value;

    static IAmazonSQS Create(SqsMessagingOptions options, IMessagingCredentialsProvider? provider)
    {
        var config = new AmazonSQSConfig();
        AwsConnection.Configure(config, options);

        // No provider and no static keys -> construct without credentials, so the SDK default chain applies.
        return AwsConnection.CreateCredentials(options, provider) is { } credentials
            ? new AmazonSQSClient(credentials, config)
            : new AmazonSQSClient(config);
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
