namespace Weasel.Core.Migrations;

/// <summary>
/// Reconnection policy options when the database is unavailable while applying database changes.
/// </summary>
/// <param name="MaxReconnectionCount">The maximum number of reconnections if the database is unavailable while applying database changes. Default is 3.</param>
/// <param name="DelayInMs">The base delay between reconnections to perform if the database is unavailable while applying database changes. Note it'll be performed with exponential backoff. Default is 50ms.</param>
public record ReconnectionOptions(
    int MaxReconnectionCount = 3,
    int DelayInMs = 50
)
{
    public static ReconnectionOptions Default { get; } = new();
}