using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging.Internals;

/// <summary>
/// Queue lookup for the SQS transport. <see cref="IQueueProvisioner"/> is the part of it a messaging
/// context needs — creating the queues it declares — and the only part the abstraction knows about.
/// </summary>
internal interface ISqsQueueProvider : IQueueProvisioner
{
    IAmazonSQS ClientFor(Type messageType);
    string ResolveName(Type messageType);
    QueueSettings GetSettings(Type messageType);
    ValueTask<string> GetQueueUrlAsync(Type messageType, CancellationToken cancellationToken);
    Task<bool> QueueExistsAsync(Type messageType, CancellationToken cancellationToken);
}

/// <summary>
/// Runtime queue lookup: physical name resolution per connection and the queue-URL cache. With
/// <see cref="SqsMessagingOptions.AutoCreateQueues"/> a missing queue is created on first use with the
/// settings declared at registration, its dead-letter queue included.
/// </summary>
internal sealed class SqsQueueProvider(
    ISqsClientFactory clientFactory,
    SqsMessagingRegistry registry,
    IOptionsMonitor<SqsMessagingOptions> optionsMonitor) : ISqsQueueProvider
{
    readonly ConcurrentDictionary<Type, string> urlCache = new();

    public IAmazonSQS ClientFor(Type messageType)
        => clientFactory.Get(registry.Get(messageType).ConnectionName);

    public string ResolveName(Type messageType) => Resolve(registry.Get(messageType));

    public QueueSettings GetSettings(Type messageType) => registry.Get(messageType).Settings;

    public ValueTask<string> GetQueueUrlAsync(Type messageType, CancellationToken cancellationToken)
        => urlCache.TryGetValue(messageType, out var url)
            ? ValueTask.FromResult(url)
            : new ValueTask<string>(ResolveOrCreateAsync(messageType, createIfMissing: null, cancellationToken));

    /// <summary>Creates the queue (dead-letter included) when missing, regardless of AutoCreateQueues — explicit provisioning.</summary>
    public Task EnsureQueueAsync(Type messageType, CancellationToken cancellationToken)
        => urlCache.ContainsKey(messageType)
            ? Task.CompletedTask
            : ResolveOrCreateAsync(messageType, createIfMissing: true, cancellationToken);

    public async Task<bool> QueueExistsAsync(Type messageType, CancellationToken cancellationToken)
    {
        var registration = registry.Get(messageType);
        var name = Resolve(registration);
        try
        {
            var url = (await clientFactory.Get(registration.ConnectionName).GetQueueUrlAsync(name, cancellationToken)).QueueUrl;
            // Seed the cache: the caller usually publishes right after checking.
            urlCache.GetOrAdd(messageType, url);
            return true;
        }
        catch (QueueDoesNotExistException)
        {
            return false;
        }
        catch (AmazonSQSException ex)
        {
            throw SqsErrors.Wrap($"Failed to resolve queue '{name}'.", ex);
        }
    }

    /// <summary>
    /// The one resolve-or-create path, shared by lazy first use and explicit provisioning.
    /// <paramref name="createIfMissing"/> is <see langword="true"/> to always create a missing queue
    /// (provisioning), or <see langword="null"/> to follow
    /// <see cref="SqsMessagingOptions.AutoCreateQueues"/>.
    /// </summary>
    async Task<string> ResolveOrCreateAsync(Type messageType, bool? createIfMissing, CancellationToken cancellationToken)
    {
        var registration = registry.Get(messageType);
        var options = OptionsFor(registration);
        var name = Resolve(registration, options);
        var client = clientFactory.Get(registration.ConnectionName);

        string queueUrl;
        try
        {
            queueUrl = (await client.GetQueueUrlAsync(name, cancellationToken)).QueueUrl;
        }
        catch (QueueDoesNotExistException ex)
        {
            if (createIfMissing is not true && !options.AutoCreateQueues)
                throw new MessagingException(
                    $"Queue '{name}' for message type {messageType.FullName} does not exist. " +
                    "Create it (e.g. via MessagingContext.EnsureQueuesAsync) or set SqsMessagingOptions.AutoCreateQueues.",
                    MessagingErrorKind.QueueNotFound, ex.ErrorCode, ex);

            queueUrl = await CreateQueueAsync(client, name, registration, options, cancellationToken);
        }
        catch (AmazonSQSException ex)
        {
            throw SqsErrors.Wrap($"Failed to resolve queue '{name}'.", ex);
        }

        // Losing the race to another thread is fine — both resolved the same queue.
        return urlCache.GetOrAdd(messageType, queueUrl);
    }

    SqsMessagingOptions OptionsFor(SqsQueueRegistration registration)
        => optionsMonitor.Get(registration.ConnectionName);

    string Resolve(SqsQueueRegistration registration) => Resolve(registration, OptionsFor(registration));

    static string Resolve(SqsQueueRegistration registration, SqsMessagingOptions options)
        => SqsQueueNames.Resolve(registration.LogicalName, registration.Settings.Fifo, options);

    async Task<string> CreateQueueAsync(
        IAmazonSQS client, string name, SqsQueueRegistration registration, SqsMessagingOptions options, CancellationToken cancellationToken)
    {
        var settings = registration.Settings;
        var attributes = BuildAttributes(settings);

        if (settings.MaxReceiveCount is int maxReceiveCount)
        {
            var dlqArn = await CreateDeadLetterQueueAsync(client, registration, options, cancellationToken);
            attributes[QueueAttributeName.RedrivePolicy] = JsonSerializer.Serialize(new Dictionary<string, string>
            {
                ["deadLetterTargetArn"] = dlqArn,
                // The SQS API defines maxReceiveCount as a string; AWS tolerates a number, strict
                // SQS-compatible services do not.
                ["maxReceiveCount"] = maxReceiveCount.ToString(CultureInfo.InvariantCulture),
            });
        }

        try
        {
            var response = await client.CreateQueueAsync(
                new CreateQueueRequest { QueueName = name, Attributes = attributes }, cancellationToken);
            return response.QueueUrl;
        }
        catch (QueueNameExistsException)
        {
            // Someone created the queue between the lookup and this call - with different attributes,
            // otherwise CreateQueue would have been idempotent. Two message types may legitimately share
            // one physical queue with different settings, so take the queue as it is.
            return (await client.GetQueueUrlAsync(name, cancellationToken)).QueueUrl;
        }
        catch (AmazonSQSException ex)
        {
            throw SqsErrors.Wrap($"Failed to create queue '{name}'.", ex);
        }
    }

    async Task<string> CreateDeadLetterQueueAsync(
        IAmazonSQS client, SqsQueueRegistration registration, SqsMessagingOptions options, CancellationToken cancellationToken)
    {
        var settings = registration.Settings;
        // The DLQ must be of the same type as the source queue, so a FIFO queue gets a FIFO DLQ.
        var dlqName = SqsQueueNames.Resolve(
            settings.DeadLetterQueueName ?? registration.LogicalName + "-dlq", settings.Fifo, options);

        var dlqAttributes = new Dictionary<string, string>();
        if (settings.Fifo)
            dlqAttributes[QueueAttributeName.FifoQueue] = "true";

        try
        {
            // CreateQueue is idempotent for an existing queue with the same attributes.
            var dlqUrl = (await client.CreateQueueAsync(
                new CreateQueueRequest { QueueName = dlqName, Attributes = dlqAttributes }, cancellationToken)).QueueUrl;

            return await GetQueueArnAsync(client, dlqUrl, cancellationToken);
        }
        catch (QueueNameExistsException)
        {
            var dlqUrl = (await client.GetQueueUrlAsync(dlqName, cancellationToken)).QueueUrl;
            return await GetQueueArnAsync(client, dlqUrl, cancellationToken);
        }
        catch (AmazonSQSException ex)
        {
            throw SqsErrors.Wrap($"Failed to create dead-letter queue '{dlqName}'.", ex);
        }
    }

    static async Task<string> GetQueueArnAsync(IAmazonSQS client, string queueUrl, CancellationToken cancellationToken)
    {
        var response = await client.GetQueueAttributesAsync(
            new GetQueueAttributesRequest { QueueUrl = queueUrl, AttributeNames = [QueueAttributeName.QueueArn] },
            cancellationToken);

        return response.QueueARN;
    }

    static Dictionary<string, string> BuildAttributes(QueueSettings settings)
    {
        var attributes = new Dictionary<string, string>();

        if (settings.Fifo)
        {
            attributes[QueueAttributeName.FifoQueue] = "true";
            if (settings.ContentBasedDeduplication)
                attributes[QueueAttributeName.ContentBasedDeduplication] = "true";
        }

        if (settings.VisibilityTimeout is TimeSpan visibility)
            attributes[QueueAttributeName.VisibilityTimeout] = Seconds(visibility);
        if (settings.MessageRetention is TimeSpan retention)
            attributes[QueueAttributeName.MessageRetentionPeriod] = Seconds(retention);
        if (settings.DeliveryDelay is TimeSpan delay)
            attributes[QueueAttributeName.DelaySeconds] = Seconds(delay);

        return attributes;
    }

    // Settings are whole seconds by validation, so nothing is truncated here.
    static string Seconds(TimeSpan value)
        => ((int)value.TotalSeconds).ToString(CultureInfo.InvariantCulture);
}
