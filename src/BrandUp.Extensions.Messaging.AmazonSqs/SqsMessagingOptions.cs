using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public class SqsMessagingOptions : IAwsConnectionOptions
{
    /// <summary>
    /// Endpoint of an SQS-compatible service, e.g. <c>https://message-queue.api.cloud.yandex.net</c>
    /// for Yandex Message Queue. When omitted, the endpoint is derived from <see cref="Region"/> —
    /// the Amazon SQS case.
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
    /// Exact physical queue names by logical name, for queues whose name the environment
    /// prefix/suffix cannot express — e.g. a shared queue owned by another team. An override is used
    /// verbatim: <see cref="QueueNamePrefix"/>/<see cref="QueueNameSuffix"/> are not applied to it
    /// (only the <c>.fifo</c> suffix is appended when missing on a FIFO queue). A logical name not
    /// listed here maps to itself with the prefix/suffix applied. Lookup is case-insensitive.
    /// </summary>
    public Dictionary<string, string> Queues { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Prefix prepended to every non-overridden queue name, e.g. <c>dev-</c> — per-environment queues without listing them.</summary>
    public string? QueueNamePrefix { get; set; }

    /// <summary>Suffix appended to every non-overridden queue name, e.g. <c>-dev</c>. Combines with <see cref="QueueNamePrefix"/>.</summary>
    public string? QueueNameSuffix { get; set; }

    /// <summary>
    /// Create missing queues on first use, applying the <see cref="QueueSettings"/> declared at
    /// registration (including the dead-letter queue). Defaults to <see langword="false"/>: a missing
    /// queue then surfaces as <see cref="MessagingException"/> with <c>IsQueueNotFound</c>.
    /// </summary>
    public bool AutoCreateQueues { get; set; }
}

internal class SqsMessagingOptionsValidator : IValidateOptions<SqsMessagingOptions>
{
    public ValidateOptionsResult Validate(string? name, SqsMessagingOptions options)
        => AwsConnection.Validate(options, nameof(SqsMessagingOptions));
}
