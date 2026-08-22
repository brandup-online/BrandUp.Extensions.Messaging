using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Options of one hosted consumer, named by <see cref="NameFor"/> (the message type's full name) in
/// the options monitor. Configure via <c>AddConsumer&lt;TMessage, THandler&gt;(options =&gt; …)</c> or bind
/// a configuration section to the named options. Validated when the consumer starts — an out-of-range
/// value fails host startup instead of looping.
/// </summary>
public class SqsConsumerOptions
{
    /// <summary>Messages requested per receive, 1–10 (the SQS cap). Defaults to 10.</summary>
    public int BatchSize { get; set; } = 10;

    /// <summary>
    /// Long-poll wait of an empty receive, up to 20 seconds (the SQS cap). Defaults to the cap — the
    /// cheapest polling with immediate delivery.
    /// </summary>
    public TimeSpan WaitTime { get; set; } = TimeSpan.FromSeconds(20);

    /// <summary>
    /// Pause after a receive that returned nothing. Not needed with the default long polling, which
    /// already waits on the server; required when <see cref="WaitTime"/> is zero, or an idle queue
    /// would be polled in a tight loop. Defaults to none.
    /// </summary>
    public TimeSpan EmptyReadDelay { get; set; } = TimeSpan.Zero;

    /// <summary>How many messages of one batch are handled concurrently, at least 1. Defaults to 1 — sequential.</summary>
    public int MaxConcurrency { get; set; } = 1;

    /// <summary>Pause after a failed receive before polling again. Defaults to 5 seconds.</summary>
    public TimeSpan PollDelayOnError { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The options-monitor name of the consumer bound to <paramref name="messageType"/> — the contract
    /// between registration, the hosted service and configuration binding.
    /// </summary>
    public static string NameFor(Type messageType) => messageType.FullName ?? messageType.Name;
}

internal class SqsConsumerOptionsValidator : IValidateOptions<SqsConsumerOptions>
{
    public ValidateOptionsResult Validate(string? name, SqsConsumerOptions options)
    {
        var consumer = string.IsNullOrEmpty(name) ? "" : $" of consumer '{name}'";

        if (options.BatchSize is < 1 or > 10)
            return ValidateOptionsResult.Fail($"{nameof(SqsConsumerOptions.BatchSize)}{consumer} must be between 1 and 10 (the SQS cap).");
        if (options.WaitTime < TimeSpan.Zero || options.WaitTime > TimeSpan.FromSeconds(20))
            return ValidateOptionsResult.Fail($"{nameof(SqsConsumerOptions.WaitTime)}{consumer} must be between 0 and 20 seconds (the SQS cap).");
        if (options.EmptyReadDelay < TimeSpan.Zero)
            return ValidateOptionsResult.Fail($"{nameof(SqsConsumerOptions.EmptyReadDelay)}{consumer} must not be negative.");
        // Without a wait on either side an idle queue is polled as fast as the network allows, and
        // every one of those empty receives is billed.
        if (options.WaitTime == TimeSpan.Zero && options.EmptyReadDelay == TimeSpan.Zero)
            return ValidateOptionsResult.Fail(
                $"{nameof(SqsConsumerOptions.WaitTime)} and {nameof(SqsConsumerOptions.EmptyReadDelay)}{consumer} cannot both be zero: " +
                "the consumer would poll an idle queue in a tight loop. Keep long polling, or set an empty-read delay.");
        if (options.MaxConcurrency < 1)
            return ValidateOptionsResult.Fail($"{nameof(SqsConsumerOptions.MaxConcurrency)}{consumer} must be at least 1.");
        if (options.PollDelayOnError < TimeSpan.Zero)
            return ValidateOptionsResult.Fail($"{nameof(SqsConsumerOptions.PollDelayOnError)}{consumer} must not be negative.");

        return ValidateOptionsResult.Success;
    }
}
