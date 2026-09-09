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
    readonly System.Collections.Concurrent.ConcurrentDictionary<string, IAmazonSQS> clients = new(StringComparer.Ordinal);

    public IAmazonSQS Get(string connectionName)
        => clients.GetOrAdd(
            connectionName,
            name => Create(options.Get(name), CredentialsRegistrations.Resolve(services, credentials, name)));

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
            client.Dispose();
        clients.Clear();
    }
}
