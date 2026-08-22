using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public class KinesisMessagingOptions : IAwsConnectionOptions
{
    /// <summary>
    /// Endpoint of a Kinesis-compatible service, e.g. <c>https://yds.serverless.yandexcloud.net</c> for
    /// Yandex Data Streams. When omitted, the endpoint is derived from <see cref="Region"/> — the Amazon
    /// Kinesis case.
    /// </summary>
    public string? ServiceUrl { get; set; }

    /// <summary>Signing region, e.g. <c>ru-central1</c> or <c>us-east-1</c>.</summary>
    public string? Region { get; set; }

    /// <summary>
    /// Static access key. Set together with <see cref="SecretAccessKey"/>, or leave both empty to use
    /// the SDK default credential chain (IAM role, environment variables, profile).
    /// </summary>
    public string? AccessKeyId { get; set; }

    public string? SecretAccessKey { get; set; }

    /// <summary>Optional session token for temporary (STS) credentials.</summary>
    public string? SessionToken { get; set; }

    /// <summary>
    /// Physical stream names by logical name. For Yandex Data Streams the physical name is the full
    /// path, e.g. <c>/ru-central1/b1g…/etn…/my-stream</c> — declare the logical name in code and map it
    /// here per environment. A logical name not listed maps to itself. Lookup is case-insensitive.
    /// (Unlike queues, streams have no name prefix/suffix — map each stream explicitly.)
    /// </summary>
    public Dictionary<string, string> Streams { get; } = new(StringComparer.OrdinalIgnoreCase);
}

internal class KinesisMessagingOptionsValidator : IValidateOptions<KinesisMessagingOptions>
{
    public ValidateOptionsResult Validate(string? name, KinesisMessagingOptions options)
        => AwsConnection.Validate(options, nameof(KinesisMessagingOptions));
}
