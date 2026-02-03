using System.Data.Common;
using Microsoft.Data.Sqlite;
using Weasel.Core;

namespace Weasel.Sqlite.Views;

/// <summary>
/// Represents a SQLite view with support for creation, deletion, and delta detection.
/// SQLite views are read-only and do not support materialized views.
/// </summary>
public class View : ISchemaObject
{
    private readonly string _viewSql;

    /// <summary>
    /// Create a view with the specified name and SQL definition
    /// </summary>
    /// <param name="viewName">Name of the view (can include schema prefix)</param>
    /// <param name="viewSql">The SELECT statement defining the view</param>
    public View(string viewName, string viewSql)
        : this(viewName != null ? DbObjectName.Parse(SqliteProvider.Instance, viewName) : throw new ArgumentNullException(nameof(viewName)), viewSql)
    {
    }

    /// <summary>
    /// Create a view with the specified identifier and SQL definition
    /// </summary>
    /// <param name="identifier">Fully qualified view name</param>
    /// <param name="viewSql">The SELECT statement defining the view</param>
    public View(DbObjectName identifier, string viewSql)
    {
        Identifier = identifier ?? throw new ArgumentNullException(nameof(identifier));
        _viewSql = viewSql ?? throw new ArgumentNullException(nameof(viewSql));
    }

    public DbObjectName Identifier { get; }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        WriteDropStatement(migrator, writer);

        var viewIdentifier = Identifier.QualifiedName;

        // Ensure SQL ends with semicolon
        var sql = _viewSql.TrimEnd();
        if (!sql.EndsWith(';'))
        {
            sql += ";";
        }

        writer.WriteLine($"CREATE VIEW {viewIdentifier} AS {sql}");
    }

    public void WriteDropStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine($"DROP VIEW IF EXISTS {Identifier.QualifiedName};");
    }

    public void ConfigureQueryCommand(Core.DbCommandBuilder builder)
    {
        // SQLite stores view definitions in sqlite_master table
        // For attached databases, we need to query the appropriate schema
        var schema = Identifier.Schema;
        var masterTable = schema == "main" ? "sqlite_master" : $"{schema}.sqlite_master";

        builder.Append($"SELECT sql FROM {masterTable} WHERE type = 'view' AND name = ");
        builder.AppendParameter(Identifier.Name);
        builder.Append(";");
    }

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var existingSql = await reader.GetFieldValueAsync<string>(0, cancellationToken).ConfigureAwait(false);

            if (!string.IsNullOrEmpty(existingSql))
            {
                // Extract just the view body (the SELECT part) from the existing SQL
                var existingBody = ExtractViewBody(existingSql);

                // Normalize both SQL statements for comparison (just compare the SELECT portion)
                var normalizedExisting = NormalizeSql(existingBody);
                var normalizedExpected = NormalizeSql(_viewSql);

                if (string.Equals(normalizedExisting, normalizedExpected, StringComparison.OrdinalIgnoreCase))
                {
                    return new SchemaObjectDelta(this, SchemaPatchDifference.None);
                }
            }
        }

        // View either doesn't exist or has changed
        return new SchemaObjectDelta(this, SchemaPatchDifference.Update);
    }

    /// <summary>
    /// Check if this view exists in the database
    /// </summary>
    public async Task<bool> ExistsInDatabaseAsync(SqliteConnection connection, CancellationToken ct = default)
    {
        await using var cmd = connection.CreateCommand();
        var builder = new Core.DbCommandBuilder(cmd);
        ConfigureQueryCommand(builder);
        builder.Compile();

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        var exists = await reader.ReadAsync(ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return exists;
    }

    /// <summary>
    /// Fetch the existing view definition from the database
    /// </summary>
    public async Task<View?> FetchExistingAsync(SqliteConnection connection, CancellationToken ct = default)
    {
        await using var cmd = connection.CreateCommand();
        var builder = new Core.DbCommandBuilder(cmd);
        ConfigureQueryCommand(builder);
        builder.Compile();

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var sql = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
            if (!string.IsNullOrEmpty(sql))
            {
                // Extract the view body from the CREATE VIEW statement
                var viewBody = ExtractViewBody(sql);
                return new View(Identifier, viewBody);
            }
        }

        return null;
    }

    /// <summary>
    /// Generate a basic CREATE VIEW SQL statement for diagnostics
    /// </summary>
    public string ToBasicCreateViewSql()
    {
        var writer = new StringWriter();
        var migrator = new SqliteMigrator { Formatting = SqlFormatting.Concise };
        WriteCreateStatement(migrator, writer);
        return writer.ToString();
    }

    /// <summary>
    /// Get the view SQL body
    /// </summary>
    public string ViewSql => _viewSql;

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

    private static string ExtractViewBody(string createViewSql)
    {
        // Extract the SELECT portion from "CREATE VIEW name AS SELECT ..."
        var asIndex = createViewSql.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
        if (asIndex >= 0)
        {
            return createViewSql.Substring(asIndex + 4).Trim();
        }

        return createViewSql;
    }
}
