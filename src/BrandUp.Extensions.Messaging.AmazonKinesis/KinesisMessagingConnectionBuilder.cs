using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// Builder of one named connection: what belongs to the account rather than to a stream — its
/// credentials provider.
/// </summary>
public class KinesisMessagingConnectionBuilder
{
    internal KinesisMessagingConnectionBuilder(IServiceCollection services, string connectionName)
    {
        Services = services;
        ConnectionName = connectionName;
    }

    public IServiceCollection Services { get; }

    /// <summary>Name of the connection being configured.</summary>
    public string ConnectionName { get; }

    /// <inheritdoc cref="KinesisMessagingBuilder.UseCredentialsProvider{TProvider}(TimeSpan?)"/>
    public KinesisMessagingConnectionBuilder UseCredentialsProvider<TProvider>(TimeSpan? refreshInterval = null)
        where TProvider : class, IMessagingCredentialsProvider
    {
        CredentialsRegistrations.Add<TProvider>(Services, ConnectionName, refreshInterval, nameof(UseCredentialsProvider));

        return this;
    }

    /// <inheritdoc cref="KinesisMessagingBuilder.UseCredentialsProvider{TProvider}(TimeSpan?)"/>
    public KinesisMessagingConnectionBuilder UseCredentialsProvider(
        Func<IServiceProvider, IMessagingCredentialsProvider> factory, TimeSpan? refreshInterval = null)
    {
        CredentialsRegistrations.Add(Services, ConnectionName, refreshInterval, factory, nameof(UseCredentialsProvider));

        return this;
    }
}
