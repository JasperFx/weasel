namespace Weasel.Sqlite.Functions;

/// <summary>
/// A collating sequence registered on a connection, used by <c>COLLATE name</c> in an
/// <c>ORDER BY</c>, a comparison, a <c>DISTINCT</c> or a <c>GROUP BY</c>. Like a function, a
/// collation lives on the connection and never in the database file, so it must be registered
/// each time a connection is opened.
/// </summary>
public sealed class SqliteCollation
{
    public SqliteCollation(string name, Comparison<string> comparison)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Collation name cannot be null or empty", nameof(name));
        }

        Name = name;
        Comparison = comparison ?? throw new ArgumentNullException(nameof(comparison));
    }

    /// <summary>
    /// The name used after <c>COLLATE</c> in SQL.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Compares two values: negative, zero or positive, as <see cref="string.Compare(string, string)" /> does.
    /// </summary>
    public Comparison<string> Comparison { get; }

    /// <summary>
    /// Register this collation on the given connection.
    /// </summary>
    public void Register(Microsoft.Data.Sqlite.SqliteConnection connection)
    {
        connection.CreateCollation(Name, Comparison);
    }
}
