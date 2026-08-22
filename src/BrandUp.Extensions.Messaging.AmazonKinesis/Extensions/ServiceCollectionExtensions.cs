using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace BrandUp.Extensions.Messaging;

public static class KinesisMessagingServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Kinesis-compatible connection — Amazon Kinesis Data Streams by region, or Yandex
    /// Data Streams by <see cref="KinesisMessagingOptions.ServiceUrl"/>. Bind message types to streams on
    /// the returned builder.
    /// </summary>
    /// <param name="services">Service collection.</param>
    /// <param name="configure">Connection options.</param>
    /// <param name="validateOnStart">
    /// Validate the connection options eagerly at host start (default). Pass <see langword="false"/> for
    /// optional messaging: validation then happens lazily, on first use.
    /// </param>
    public static KinesisMessagingBuilder AddKinesisMessaging(
        this IServiceCollection services, Action<KinesisMessagingOptions> configure, bool validateOnStart = true)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var builder = services.AddOptions<KinesisMessagingOptions>().Configure(configure);
        if (validateOnStart)
            builder.ValidateOnStart();

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<KinesisMessagingOptions>, KinesisMessagingOptionsValidator>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<KinesisConsumerOptions>, KinesisConsumerOptionsValidator>());

        services.TryAddSingleton<IMessageSerializer, JsonMessageSerializer>();
        services.TryAddSingleton<IKinesisClientFactory, KinesisClientFactory>();
        services.TryAddSingleton<IMessagePublisher, ServiceProviderMessagePublisher>();

        return new KinesisMessagingBuilder(services);
    }
}
