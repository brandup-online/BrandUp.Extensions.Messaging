using System.Runtime.CompilerServices;

namespace BrandUp.Extensions.Messaging.Integration;

/// <summary>
/// A <see cref="FactAttribute"/> that is skipped unless SQS connection settings are present in the
/// environment. Locally (no <c>SQS_SERVICE_URL</c>) the test is reported as skipped; in CI, where the
/// pipeline starts ElasticMQ and sets the variables, it runs.
/// </summary>
public sealed class SqsFactAttribute : FactAttribute
{
    public SqsFactAttribute(
        [CallerFilePath] string? sourceFilePath = null,
        [CallerLineNumber] int sourceLineNumber = -1)
        : base(sourceFilePath, sourceLineNumber)
    {
        if (!SqsEnvironment.IsConfigured)
            Skip = SqsEnvironment.SkipReason;
    }
}
