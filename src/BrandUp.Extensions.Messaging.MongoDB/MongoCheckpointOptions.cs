using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace BrandUp.Extensions.Messaging;

public class MongoCheckpointOptions
{
    /// <summary>
    /// Collection holding the read positions of stream consumers: one small document per shard and
    /// consumer group. Named after the library that owns it, so it reads as such next to the other
    /// BrandUp collections of a database.
    /// </summary>
    public string CollectionName { get; set; } = "brandup.messaging.checkpoints";

    /// <summary>
    /// Resolves the database holding the collection; by default <see cref="IMongoDatabase"/> is taken
    /// from the service provider.
    /// </summary>
    public Func<IServiceProvider, IMongoDatabase>? DatabaseAccessor { get; set; }
}

internal class MongoCheckpointOptionsValidator : IValidateOptions<MongoCheckpointOptions>
{
    public ValidateOptionsResult Validate(string? name, MongoCheckpointOptions options)
        => string.IsNullOrWhiteSpace(options.CollectionName)
            ? ValidateOptionsResult.Fail($"{nameof(MongoCheckpointOptions.CollectionName)} is required.")
            : ValidateOptionsResult.Success;
}
