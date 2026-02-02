namespace Weasel.Core.Tables;

/// <summary>
/// Common interface for database objects that have a name
/// </summary>
public interface INamed
{
    string Name { get; }
}
