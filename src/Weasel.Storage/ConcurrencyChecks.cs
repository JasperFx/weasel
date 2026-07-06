namespace Weasel.Storage;

/// <summary>
///     Whether a storage session honors optimistic concurrency checks when building document
///     update/upsert operations.
/// </summary>
public enum ConcurrencyChecks
{
    /// <summary>
    ///     Optimistic concurrency checks are enforced (Default)
    /// </summary>
    Enabled,

    /// <summary>
    ///     Optimistic concurrency checks are disabled for this session
    /// </summary>
    Disabled
}
