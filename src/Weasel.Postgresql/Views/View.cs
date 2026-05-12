using System.Data.Common;
using Npgsql;
using Weasel.Core;

namespace Weasel.Postgresql.Views;

public class View: ViewBase
{
    public View(string viewName, string viewSql)
        : this(DbObjectName.Parse(PostgresqlProvider.Instance, viewName), viewSql)
    {
    }

    public View(DbObjectName name, string viewSql)
        : base(PostgresqlObjectName.From(name ?? throw new ArgumentNullException(nameof(name))), viewSql)
    {
    }

    protected virtual string ViewType => "VIEW";

    protected virtual char ViewKind => 'v';

    protected virtual string GetCreationOptions() => string.Empty;

    /// <inheritdoc />
    protected override DbObjectName WithSchema(string schemaName)
        => PostgresqlObjectName.From(new DbObjectName(schemaName, Identifier.Name));

    /// <inheritdoc />
    protected override Migrator GetDefaultMigratorForBasicSql()
        => new PostgresqlMigrator { Formatting = SqlFormatting.Concise };

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        WriteDropStatement(migrator, writer);

        var viewIdentifier = Identifier.QualifiedName;
        var creationOptions = GetCreationOptions();

        var viewIdentifierWithCreationOptions = string.IsNullOrWhiteSpace(creationOptions)
            ? viewIdentifier
            : $"{viewIdentifier} {creationOptions}";
        writer.WriteLine($"CREATE {ViewType} {viewIdentifierWithCreationOptions} AS {ViewSql};");
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"DROP {ViewType} IF EXISTS {Identifier.QualifiedName};");
    }

    public override void ConfigureQueryCommand(Core.DbCommandBuilder builder)
    {
        builder.Append("SELECT (CASE WHEN pg_has_role(c.relowner, 'USAGE'::text) THEN LTRIM(pg_get_viewdef(c.oid),' ') ELSE NULL::text END)::information_schema.character_data AS view_definition ");
        builder.Append("FROM pg_catalog.pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace ");
        builder.Append("WHERE c.relkind = '");
        builder.Append(ViewKind);
        builder.Append("' AND n.nspname = ");
        builder.AppendParameter(Identifier.Schema);
        builder.Append(" AND c.relname = ");
        builder.AppendParameter(Identifier.Name);
        builder.Append(";");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        if (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var previousView = reader.GetString(0);
            //This is to support when users specify view SQL with/without colon. Postgres allways returns with semicolon.
            var sanitizedViewSqlBody = ViewSql.EndsWith(';') ? ViewSql : ViewSql + ";";
            if (string.Equals(previousView, sanitizedViewSqlBody, StringComparison.OrdinalIgnoreCase))
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
