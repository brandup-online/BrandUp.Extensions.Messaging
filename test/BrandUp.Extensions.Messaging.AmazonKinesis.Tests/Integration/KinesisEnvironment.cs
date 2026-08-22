using System.Runtime.CompilerServices;

namespace BrandUp.Extensions.Messaging.Integration;

/// <summary>
/// Reads Kinesis connection settings from environment variables. Without <c>KINESIS_SERVICE_URL</c>
/// the integration tests are skipped, so the suite stays green locally without a running LocalStack.
/// </summary>
static class KinesisEnvironment
{
    public const string SkipReason = "KINESIS_SERVICE_URL is not configured; skipping Kinesis integration test.";

    public static string? ServiceUrl => Environment.GetEnvironmentVariable("KINESIS_SERVICE_URL");
    public static string? AccessKey => Environment.GetEnvironmentVariable("KINESIS_ACCESS_KEY");
    public static string? SecretKey => Environment.GetEnvironmentVariable("KINESIS_SECRET_KEY");
    public static string Region => Environment.GetEnvironmentVariable("KINESIS_REGION") ?? "us-east-1";

    public static bool IsConfigured => !string.IsNullOrEmpty(ServiceUrl);

    public static void Apply(KinesisMessagingOptions options)
    {
        options.ServiceUrl = ServiceUrl;
        options.Region = Region;
        options.AccessKeyId = AccessKey ?? "test";
        options.SecretAccessKey = SecretKey ?? "test";
    }
}

public sealed class KinesisFactAttribute : FactAttribute
{
    public KinesisFactAttribute(
        [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1)
        : base(sourceFilePath, sourceLineNumber)
    {
        if (!KinesisEnvironment.IsConfigured)
            Skip = KinesisEnvironment.SkipReason;
    }
}
