using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace BrandUp.Extensions.Messaging;

public static class MongoMessagingServiceCollectionExtensions
{
    /// <summary>
    /// Registers the MongoDB <see cref="ICheckpointStore"/> — where stream readers keep their position.
    /// The database comes from <see cref="MongoCheckpointOptions.DatabaseAccessor"/>, or from
    /// <see cref="IMongoDatabase"/> in the service provider when it is not set.
    /// </summary>
    public static IServiceCollection AddMongoMessagingCheckpoints(
        this IServiceCollection services, Action<MongoCheckpointOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        AddValidatedOptions<MongoCheckpointOptions, MongoCheckpointOptionsValidator>(services, configure);

        services.TryAddSingleton<ICheckpointStore>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<MongoCheckpointOptions>>().Value;
            var database = Database(
                sp, options.DatabaseAccessor, "checkpoint store", nameof(MongoCheckpointOptions));

            return new MongoCheckpointStore(database.GetCollection<CheckpointDocument>(options.CollectionName));
        });

        return services;
    }

    /// <summary>
    /// Registers the MongoDB <see cref="IShardLeaseStore"/> — what lets several instances of one
    /// consumer group read a stream together: each shard is leased to one of them, an instance that
    /// stops has its shards taken over once the lease expires, and the readers even out their load by
    /// handing shards over. Without it a group must be read by a single instance.
    /// <para>
    /// The database comes from <see cref="MongoShardLeaseOptions.DatabaseAccessor"/>, or from
    /// <see cref="IMongoDatabase"/> in the service provider when it is not set.
    /// </para>
    /// </summary>
    public static IServiceCollection AddMongoMessagingShardLeases(
        this IServiceCollection services, Action<MongoShardLeaseOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        AddValidatedOptions<MongoShardLeaseOptions, MongoShardLeaseOptionsValidator>(services, configure);

        services.TryAddSingleton<IShardLeaseStore>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<MongoShardLeaseOptions>>().Value;
            var database = Database(
                sp, options.DatabaseAccessor, "shard lease store", nameof(MongoShardLeaseOptions));

            return new MongoShardLeaseStore(database.GetCollection<ShardLeaseDocument>(options.CollectionName));
        });

        return services;
    }

    /// <summary>
    /// Options validated at host start, so a bad collection name is an error on startup rather than on
    /// the first read.
    /// </summary>
    static void AddValidatedOptions<TOptions, TValidator>(IServiceCollection services, Action<TOptions>? configure)
        where TOptions : class
        where TValidator : class, IValidateOptions<TOptions>
    {
        var builder = services.AddOptions<TOptions>();
        if (configure is not null)
            builder.Configure(configure);
        builder.ValidateOnStart();

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<TOptions>, TValidator>());
    }

    /// <summary>
    /// The database a store lives in: the configured accessor, or the registered
    /// <see cref="IMongoDatabase"/>. Both stores resolve it the same way, and say the same thing when
    /// there is neither — naming the accessor to set.
    /// </summary>
    static IMongoDatabase Database(
        IServiceProvider provider, Func<IServiceProvider, IMongoDatabase>? accessor, string store, string optionsName)
        => accessor?.Invoke(provider)
            ?? provider.GetService<IMongoDatabase>()
            ?? throw new InvalidOperationException(
                $"No {nameof(IMongoDatabase)} is registered for the {store}; register one or set " +
                $"{optionsName}.{nameof(MongoCheckpointOptions.DatabaseAccessor)}.");
}
