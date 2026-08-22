using Amazon.Kinesis;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

internal interface IKinesisClientFactory
{
    IAmazonKinesis Get();
}

internal sealed class KinesisClientFactory(IOptions<KinesisMessagingOptions> options) : IKinesisClientFactory, IDisposable
{
    readonly Lazy<IAmazonKinesis> client = new(() => Create(options.Value), LazyThreadSafetyMode.ExecutionAndPublication);

    public IAmazonKinesis Get() => client.Value;

    static IAmazonKinesis Create(KinesisMessagingOptions options)
    {
        var config = new AmazonKinesisConfig();
        AwsConnection.Configure(config, options);

        // No static keys -> construct without credentials, so the SDK default chain applies.
        return AwsConnection.CreateCredentials(options) is { } credentials
            ? new AmazonKinesisClient(credentials, config)
            : new AmazonKinesisClient(config);
    }

    public void Dispose()
    {
        if (client.IsValueCreated)
            client.Value.Dispose();
    }
}
