// Linked as a shared source file into every AWS provider package (AmazonSqs, AmazonKinesis): the
// packages have no common AWS-referencing assembly, and the Abstraction package must stay free of the
// AWS SDK, so the connection bootstrap is shared at the source level.
using Amazon;
using Amazon.Runtime;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>Connection options common to every AWS-protocol provider.</summary>
internal interface IAwsConnectionOptions
{
    string? ServiceUrl { get; }
    string? Region { get; }
    string? AccessKeyId { get; }
    string? SecretAccessKey { get; }
    string? SessionToken { get; }
}

internal static class AwsConnection
{
    /// <summary>
    /// The one validation rule of an AWS connection: an endpoint (ServiceUrl or Region) is required;
    /// access keys are either both set or both omitted — omitted means the SDK default credential
    /// chain (IAM role, environment, profile).
    /// </summary>
    public static ValidateOptionsResult Validate(IAwsConnectionOptions options, string optionsTypeName)
    {
        if (string.IsNullOrEmpty(options.ServiceUrl) && string.IsNullOrEmpty(options.Region))
            return ValidateOptionsResult.Fail(
                $"Either {optionsTypeName}.{nameof(IAwsConnectionOptions.ServiceUrl)} or {optionsTypeName}.{nameof(IAwsConnectionOptions.Region)} is required.");

        if (string.IsNullOrEmpty(options.AccessKeyId) != string.IsNullOrEmpty(options.SecretAccessKey))
            return ValidateOptionsResult.Fail(
                $"{optionsTypeName}: set both {nameof(IAwsConnectionOptions.AccessKeyId)} and {nameof(IAwsConnectionOptions.SecretAccessKey)}, " +
                "or neither to use the SDK default credential chain (IAM role, environment, profile).");

        return ValidateOptionsResult.Success;
    }

    /// <summary>Applies the endpoint: an explicit ServiceUrl (SQS/Kinesis-compatible services), else the AWS region.</summary>
    public static void Configure(ClientConfig config, IAwsConnectionOptions options)
    {
        if (!string.IsNullOrEmpty(options.ServiceUrl))
        {
            config.ServiceURL = options.ServiceUrl;
            if (!string.IsNullOrEmpty(options.Region))
                config.AuthenticationRegion = options.Region;
        }
        else
        {
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(options.Region);
        }
    }

    /// <summary>
    /// Credentials of a connection, in order: a registered provider (renewed in place), then static keys
    /// from the options, then <see langword="null"/> — construct the client without credentials, so the
    /// SDK default credential chain applies (IAM role, environment, profile).
    /// </summary>
    public static AWSCredentials? CreateCredentials(IAwsConnectionOptions options, IMessagingCredentialsProvider? provider = null)
    {
        if (provider is not null)
            return new ProviderCredentials(provider);

        if (string.IsNullOrEmpty(options.AccessKeyId))
            return null;

        return string.IsNullOrEmpty(options.SessionToken)
            ? new BasicAWSCredentials(options.AccessKeyId, options.SecretAccessKey)
            : new SessionAWSCredentials(options.AccessKeyId, options.SecretAccessKey, options.SessionToken);
    }

    /// <summary>
    /// Feeds an <see cref="IMessagingCredentialsProvider"/> to the SDK. The SDK re-reads the provider
    /// once the credentials it was given expire, which is what lets one client outlive many rotations:
    /// the client is never rebuilt, only its credentials are. The read is synchronous — the provider
    /// serves its cache, and the refresher behind it keeps that cache warm.
    /// </summary>
    sealed class ProviderCredentials(IMessagingCredentialsProvider provider) : RefreshingAWSCredentials
    {
        protected override CredentialsRefreshState GenerateNewCredentials()
        {
            var credentials = provider.GetCurrent()
                ?? throw new MessagingException($"{provider.GetType().Name} returned no credentials.");

            if (string.IsNullOrEmpty(credentials.AccessKeyId) || string.IsNullOrEmpty(credentials.SecretAccessKey))
                throw new MessagingException(
                    $"{provider.GetType().Name} returned credentials without an access key. " +
                    "A provider must serve what its last refresh produced; check that RefreshAsync ran before the first publish.");

            // Expired credentials would be refused by the SDK with an error about its own refresh cycle,
            // which says nothing about what to fix. The cache went stale: either RefreshAsync is failing
            // (it is logged), or it renews too late for the lifetime the provider hands out.
            if (credentials.ExpiresUtc is { } expiresUtc && expiresUtc <= DateTimeOffset.UtcNow)
                throw new MessagingException(
                    $"{provider.GetType().Name} returned credentials that expired at {expiresUtc:u}. " +
                    "Renew them in RefreshAsync before they expire, or hand out a longer lifetime.");

            // Credentials without an expiry are re-read hourly rather than never: a provider that rotates
            // silently then still takes effect, and an hour of caching costs nothing.
            return new CredentialsRefreshState(
                new ImmutableCredentials(credentials.AccessKeyId, credentials.SecretAccessKey, credentials.SessionToken),
                (credentials.ExpiresUtc ?? DateTimeOffset.UtcNow.AddHours(1)).UtcDateTime);
        }
    }
}
