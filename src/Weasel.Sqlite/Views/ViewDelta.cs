using Weasel.Core;

namespace Weasel.Sqlite.Views;

/// <summary>
/// Represents the difference between expected and actual view definitions.
/// Inherits the standard <see cref="SchemaObjectDelta{T}" /> boilerplate
/// (Expected/Actual properties, Difference computation via the protected
/// <see cref="compare" /> hook, and the SchemaObject = Expected mapping) and only
/// overrides the parts that are SQLite-view-specific:
/// <list type="bullet">
///   <item><see cref="compare" /> uses whitespace-insensitive SQL comparison via
///         <see cref="View.NormalizeSql" />.</item>
///   <item><see cref="WriteUpdate" /> delegates to <see cref="View.WriteCreateStatement" />,
///         which already emits a DROP+CREATE pair (SQLite has no ALTER VIEW).</item>
///   <item><see cref="WriteRollback" /> handles the "view didn't exist before" case by
///         dropping the expected view rather than NRE'ing on a null Actual.</item>
///   <item><see cref="WriteRestorationOfPreviousState" /> is a no-op when there was no
///         previous state (Actual is null) instead of throwing.</item>
/// </list>
/// </summary>
public class ViewDelta : SchemaObjectDelta<View>
{
    public ViewDelta(View expected, View? actual) : base(expected, actual)
    {
    }

    protected override SchemaPatchDifference compare(View expected, View? actual)
    {
        // View doesn't exist in database
        if (actual == null)
        {
            return SchemaPatchDifference.Create;
        }

        // Compare normalized SQL
        var expectedSql = View.NormalizeSql(expected.ViewSql);
        var actualSql = View.NormalizeSql(actual.ViewSql);

        if (string.Equals(expectedSql, actualSql, StringComparison.OrdinalIgnoreCase))
        {
            return SchemaPatchDifference.None;
        }

        // View exists but definition is different
        return SchemaPatchDifference.Update;
    }

    public override void WriteUpdate(Migrator migrator, TextWriter writer)
    {
        if (Difference == SchemaPatchDifference.None)
        {
            return;
        }

        // For both Create and Update, we drop and recreate the view
        // SQLite doesn't support ALTER VIEW, so we always DROP and CREATE
        // (View.WriteCreateStatement emits DROP IF EXISTS then CREATE.)
        Expected.WriteCreateStatement(migrator, writer);
    }

    public override void WriteRollback(Migrator migrator, TextWriter writer)
    {
        if (Actual != null)
        {
            // Restore the previous view definition
            Actual.WriteCreateStatement(migrator, writer);
        }
        else
        {
            // View didn't exist before, so drop it
            Expected.WriteDropStatement(migrator, writer);
        }
    }

    public override void WriteRestorationOfPreviousState(Migrator migrator, TextWriter writer)
    {
        if (Actual != null)
        {
            // Restore the previous view definition as it existed
            Actual.WriteCreateStatement(migrator, writer);
        }
        // else: no previous state to restore — no-op
    }
}
