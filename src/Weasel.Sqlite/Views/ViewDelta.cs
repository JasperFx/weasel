using Weasel.Core;

namespace Weasel.Sqlite.Views;

/// <summary>
/// Represents the difference between expected and actual view definitions
/// </summary>
public class ViewDelta : ISchemaObjectDelta
{
    private readonly View _expected;
    private readonly View? _actual;

    public ViewDelta(View expected, View? actual)
    {
        _expected = expected ?? throw new ArgumentNullException(nameof(expected));
        _actual = actual;
    }

    public ISchemaObject SchemaObject => _expected;

    public SchemaPatchDifference Difference
    {
        get
        {
            // View doesn't exist in database
            if (_actual == null)
            {
                return SchemaPatchDifference.Create;
            }

            // Compare normalized SQL
            var expectedSql = NormalizeSql(_expected.ViewSql);
            var actualSql = NormalizeSql(_actual.ViewSql);

            if (string.Equals(expectedSql, actualSql, StringComparison.OrdinalIgnoreCase))
            {
                return SchemaPatchDifference.None;
            }

            // View exists but definition is different
            return SchemaPatchDifference.Update;
        }
    }

    public void WriteUpdate(Migrator migrator, TextWriter writer)
    {
        if (Difference == SchemaPatchDifference.None)
        {
            return;
        }

        // For both Create and Update, we drop and recreate the view
        // SQLite doesn't support ALTER VIEW, so we always DROP and CREATE
        _expected.WriteCreateStatement(migrator, writer);
    }

    public void WriteRollback(Migrator migrator, TextWriter writer)
    {
        if (_actual != null)
        {
            // Restore the previous view definition
            _actual.WriteCreateStatement(migrator, writer);
        }
        else
        {
            // View didn't exist before, so drop it
            _expected.WriteDropStatement(migrator, writer);
        }
    }

    public void WriteRestorationOfPreviousState(Migrator migrator, TextWriter writer)
    {
        if (_actual != null)
        {
            // Restore the previous view definition as it existed
            _actual.WriteCreateStatement(migrator, writer);
        }
    }

    private static string NormalizeSql(string sql)
    {
        // Remove all whitespace for comparison purposes
        var normalized = sql
            .Replace("\r\n", "")
            .Replace("\n", "")
            .Replace("\r", "")
            .Replace("\t", "")
            .Replace(" ", "")
            .Trim()
            .TrimEnd(';');

        return normalized;
    }
}
