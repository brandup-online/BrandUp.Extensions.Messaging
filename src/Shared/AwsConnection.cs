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
    /// Static credentials from the options, or <see langword="null"/> when no keys are set — construct
    /// the client without credentials then, so the SDK default credential chain applies.
    /// </summary>
    public static AWSCredentials? CreateCredentials(IAwsConnectionOptions options)
    {
        if (string.IsNullOrEmpty(options.AccessKeyId))
            return null;

        return string.IsNullOrEmpty(options.SessionToken)
            ? new BasicAWSCredentials(options.AccessKeyId, options.SecretAccessKey)
            : new SessionAWSCredentials(options.AccessKeyId, options.SecretAccessKey, options.SessionToken);
    }
}
