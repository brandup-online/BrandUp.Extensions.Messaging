using Amazon.Kinesis;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

internal interface IKinesisClientFactory
{
    IAmazonKinesis Get();
}

internal sealed class KinesisClientFactory : IKinesisClientFactory, IDisposable
{
    readonly Lazy<IAmazonKinesis> client;

    public KinesisClientFactory(
        IOptions<KinesisMessagingOptions> options,
        IServiceProvider services,
        IEnumerable<MessagingCredentialsRegistration> credentials)
    {
        client = new(
            () => Create(options.Value, CredentialsRegistrations.Resolve(services, credentials, Options.DefaultName)),
            LazyThreadSafetyMode.ExecutionAndPublication);
    }

    public IAmazonKinesis Get() => client.Value;

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
        if (client.IsValueCreated)
            client.Value.Dispose();
    }
}
