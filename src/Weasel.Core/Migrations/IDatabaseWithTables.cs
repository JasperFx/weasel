namespace Weasel.Core.Migrations;

/// <summary>
///     An IDatabase that supports adding tables and running migrations without subclassing
/// </summary>
public interface IDatabaseWithTables: IDatabase
{
    IReadOnlyList<ITable> Tables { get; }
    ITable AddTable(DbObjectName identifier);
}
