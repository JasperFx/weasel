using System.Data.Common;
using Npgsql;
using Weasel.Core;

namespace Weasel.Postgresql.Views;

public class View: ISchemaObject
{
    private readonly string viewSql;

    public View(string viewName, string viewSql): this(DbObjectName.Parse(PostgresqlProvider.Instance, viewName), viewSql)
    {
    }

    public View(DbObjectName name, string viewSql)
    {
        Identifier = name ?? throw new ArgumentNullException(nameof(name));
        this.viewSql = viewSql ?? throw new ArgumentNullException(nameof(viewSql));
    }

    public DbObjectName Identifier { get; private set; }

    protected virtual string ViewType => "VIEW";

    protected virtual char ViewKind => 'v';

    protected virtual string GetCreationOptions() => string.Empty;

    /// <summary>
    ///     Mutate this view to change the identifier to being in a different schema
    /// </summary>
    /// <param name="schemaName"></param>
    public void MoveToSchema(string schemaName)
    {
        var identifier = new PostgresqlObjectName(schemaName, Identifier.Name);
        Identifier = identifier;
    }

    /// <summary>
    ///     Generate the CREATE VIEW SQL expression with default
    ///     DDL rules. This is useful for quick diagnostics
    /// </summary>
    /// <returns></returns>
    public string ToBasicCreateViewSql()
    {
        var writer = new StringWriter();
        var rules = new PostgresqlMigrator { Formatting = SqlFormatting.Concise };
        WriteCreateStatement(rules, writer);

        return writer.ToString();
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        WriteDropStatement(migrator, writer);

        var viewIdentifier = Identifier.QualifiedName;
        var creationOptions = GetCreationOptions();

        var viewIdentifierWithCreationOptions = string.IsNullOrWhiteSpace(creationOptions)
            ? viewIdentifier
            : $"{viewIdentifier} {GetCreationOptions()}";
        writer.WriteLine($"CREATE {ViewType} {viewIdentifierWithCreationOptions} AS {viewSql};");
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP {ViewType} IF EXISTS {Identifier.QualifiedName};");
    }

    public void ConfigureQueryCommand(Core.DbCommandBuilder builder)
    {
        builder.Append("SELECT (CASE WHEN pg_has_role(c.relowner, 'USAGE'::text) THEN pg_get_viewdef(c.oid) ELSE NULL::text END)::information_schema.character_data AS view_definition ");
        builder.Append("FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace ");
        builder.Append("WHERE c.relkind = '");
        builder.Append(ViewKind);
        builder.Append("' AND n.nspname = ");
        builder.AppendParameter(Identifier.Schema);
        builder.Append(" AND c.relname = ");
        builder.AppendParameter(Identifier.Name);
        builder.Append(";");
    }

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var previousView = reader.GetString(0);
            if (string.Equals(previousView, viewSql, StringComparison.OrdinalIgnoreCase))
            {
                return new SchemaObjectDelta(this, SchemaPatchDifference.None);
            }
        }
        return new SchemaObjectDelta(this, SchemaPatchDifference.Update);
    }

    public async Task<bool> ExistsInDatabaseAsync(NpgsqlConnection conn, CancellationToken ct = default)
    {
        await using var cmd = conn.CreateCommand();
        var builder = new Core.DbCommandBuilder(cmd);
        ConfigureQueryCommand(builder);
        builder.Compile();

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        var any = await reader.ReadAsync(ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return any;
    }
}
