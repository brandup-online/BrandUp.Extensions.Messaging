namespace BrandUp.Extensions.Messaging.Integration;

/// <summary>
/// Reads SQS connection settings from environment variables. When <see cref="ServiceUrl"/> is not set
/// the integration tests are skipped, so the suite stays green locally without a running ElasticMQ.
/// </summary>
static class SqsEnvironment
{
    public const string SkipReason = "SQS_SERVICE_URL is not configured; skipping SQS integration test.";

    public static string? ServiceUrl => Environment.GetEnvironmentVariable("SQS_SERVICE_URL");
    public static string? AccessKey => Environment.GetEnvironmentVariable("SQS_ACCESS_KEY");
    public static string? SecretKey => Environment.GetEnvironmentVariable("SQS_SECRET_KEY");
    public static string Region => Environment.GetEnvironmentVariable("SQS_REGION") ?? "elasticmq";

    public static bool IsConfigured => !string.IsNullOrEmpty(ServiceUrl);

    /// <summary>The single place the connection settings are applied, shared by every integration test.</summary>
    public static void Apply(SqsMessagingOptions options)
    {
        options.ServiceUrl = ServiceUrl;
        options.Region = Region;
        options.AccessKeyId = AccessKey ?? "x";
        options.SecretAccessKey = SecretKey ?? "x";
        options.AutoCreateQueues = true;
    }
}
