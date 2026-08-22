using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace BrandUp.Extensions.Messaging;

public static class MongoCheckpointServiceCollectionExtensions
{
    /// <summary>
    /// Registers the MongoDB <see cref="ICheckpointStore"/> — where stream readers keep their position.
    /// The database comes from <see cref="MongoCheckpointOptions.DatabaseAccessor"/>, or from
    /// <see cref="IMongoDatabase"/> in the service provider when it is not set.
    /// </summary>
    public static IServiceCollection AddMongoCheckpoints(
        this IServiceCollection services, Action<MongoCheckpointOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var builder = services.AddOptions<MongoCheckpointOptions>();
        if (configure is not null)
            builder.Configure(configure);
        builder.ValidateOnStart();

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<MongoCheckpointOptions>, MongoCheckpointOptionsValidator>());

        services.TryAddSingleton<ICheckpointStore>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<MongoCheckpointOptions>>().Value;
            var database = options.DatabaseAccessor?.Invoke(sp)
                ?? sp.GetService<IMongoDatabase>()
                ?? throw new InvalidOperationException(
                    $"No {nameof(IMongoDatabase)} is registered for the checkpoint store; register one or set " +
                    $"{nameof(MongoCheckpointOptions)}.{nameof(MongoCheckpointOptions.DatabaseAccessor)}.");

            return new MongoCheckpointStore(database.GetCollection<CheckpointDocument>(options.CollectionName));
        });

        return services;
    }
}
