using Microsoft.Extensions.Options;
using MongoDB.Driver;

namespace BrandUp.Extensions.Messaging;

public class MongoShardLeaseOptions
{
    /// <summary>
    /// Collection holding the shard leases of stream consumers: one small document per shard and
    /// consumer group, deleted when the shard is given up. Named after the library that owns it, like
    /// the checkpoint collection next to it.
    /// </summary>
    public string CollectionName { get; set; } = "brandup.messaging.leases";

    /// <summary>
    /// Resolves the database holding the collection; by default <see cref="IMongoDatabase"/> is taken
    /// from the service provider.
    /// </summary>
    public Func<IServiceProvider, IMongoDatabase>? DatabaseAccessor { get; set; }
}

internal class MongoShardLeaseOptionsValidator : IValidateOptions<MongoShardLeaseOptions>
{
    public ValidateOptionsResult Validate(string? name, MongoShardLeaseOptions options)
        => string.IsNullOrWhiteSpace(options.CollectionName)
            ? ValidateOptionsResult.Fail($"{nameof(MongoShardLeaseOptions.CollectionName)} is required.")
            : ValidateOptionsResult.Success;
}
