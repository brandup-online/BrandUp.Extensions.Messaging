using System.Collections.Concurrent;
using BrandUp.Extensions.Messaging.Internals;
using Microsoft.Extensions.DependencyInjection;

namespace BrandUp.Extensions.Messaging;

/// <summary>
/// <see cref="ICheckpointStore"/> in a dictionary: what a stream reader needs in tests, and inspectable
/// so a test can assert on the position a reader committed.
/// </summary>
public class InMemoryCheckpointStore : ICheckpointStore
{
    readonly ConcurrentDictionary<CheckpointKey, Checkpoint> checkpoints = new();

    /// <summary>Everything committed so far, keyed by stream, consumer group and shard.</summary>
    public IReadOnlyDictionary<CheckpointKey, Checkpoint> Checkpoints => checkpoints;

    public Task<Checkpoint?> GetAsync(CheckpointKey key, CancellationToken cancellationToken = default)
        => Task.FromResult(checkpoints.GetValueOrDefault(key));

    public Task SetAsync(CheckpointKey key, Checkpoint checkpoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);

        checkpoints[key] = checkpoint;
        return Task.CompletedTask;
    }

    public void Clear() => checkpoints.Clear();
}

public static class InMemoryCheckpointServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="InMemoryCheckpointStore"/> as the checkpoint store for tests. Repeated
    /// calls reuse the same instance, so what a test inspects is what the reader writes to.
    /// </summary>
    public static IServiceCollection AddInMemoryCheckpoints(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (RegistrationGuards.FindInstance<InMemoryCheckpointStore>(services) is not null)
            return services;

        var store = new InMemoryCheckpointStore();
        services.AddSingleton(store);
        services.AddSingleton<ICheckpointStore>(store);

        return services;
    }
}
