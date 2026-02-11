using System.Data.Common;
using System.Text;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using Weasel.Postgresql;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Postgresql.Functions;

public class Function: ISchemaObject
{
    private readonly string? _body;
    private readonly string[]? _dropStatements;

    public Function(DbObjectName identifier, string body, string[] dropStatements)
    {
        _body = body;
        _dropStatements = dropStatements;
        Identifier = PostgresqlObjectName.From(identifier, SchemaUtils.IdentifierUsage.Function);
    }

    public Function(DbObjectName identifier, string? body)
    {
        _body = body;
        Identifier = PostgresqlObjectName.From(identifier, SchemaUtils.IdentifierUsage.Function);
    }

    protected Function(DbObjectName identifier)
    {
        Identifier = PostgresqlObjectName.From(identifier, SchemaUtils.IdentifierUsage.Function);
    }


    public bool IsRemoved { get; protected set; }


    public virtual void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine(_body);
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        foreach (var dropStatement in DropStatements()) writer.WriteLine(dropStatement);
    }

    public DbObjectName Identifier { get; }

    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;

        builder.Append($@"
SELECT pg_get_functiondef(pg_proc.oid)
FROM pg_proc JOIN pg_namespace as ns ON pg_proc.pronamespace = ns.oid WHERE ns.nspname = :{schemaParam} and proname = :{nameParam};

SELECT format('DROP FUNCTION IF EXISTS %s.%s(%s);'
             ,n.nspname
             ,p.proname
             ,pg_get_function_identity_arguments(p.oid))
FROM   pg_proc p
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE  p.proname = :{nameParam}
AND    n.nspname = :{schemaParam};
");
    }

    public async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readExistingAsync(reader, ct).ConfigureAwait(false);
        return new FunctionDelta(this, existing);
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }

    public static string ParseSignature(string body)
    {
        var functionIndex = body.IndexOf("FUNCTION", StringComparison.OrdinalIgnoreCase);
        var openParen = body.IndexOf('(');
        var closeParen = body.IndexOf(')');

        var stringBuilder = new StringBuilder();

        var nameStart = functionIndex + "function".Length;
        var funcName = body.AsSpan(nameStart, openParen - nameStart).Trim();

        stringBuilder.Append(funcName);
        stringBuilder.Append('(');

        var argsSpan = body.AsSpan(openParen + 1, closeParen - openParen - 1).Trim();
        var argsEnumerator = argsSpan.Split(',');

        foreach (var range in argsEnumerator)
        {
            // doc jsonb | docdotnettype character varying | docid uuid | docversion uuid
            var span = argsSpan[range].Trim();

            var indexOfType = span.IndexOf(' ') + 1;

            stringBuilder.Append(span[indexOfType..]);
            if (range.End.Value != argsSpan.Length)
            {
                stringBuilder.Append(", ");
            }
        }

        stringBuilder.Append(')');
        return stringBuilder.ToString();
    }

    public static DbObjectName ParseIdentifier(string functionSql)
    {
        var signature = ParseSignature(functionSql);
        var open = signature.IndexOf('(');
        return DbObjectName.Parse(PostgresqlProvider.Instance, signature.Substring(0, open));
    }

    public async Task<Function?> FetchExistingAsync(NpgsqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);

        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var result = await readExistingAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return result;
    }

    private async Task<Function?> readExistingAsync(DbDataReader reader, CancellationToken ct = default)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            await reader.NextResultAsync(ct).ConfigureAwait(false);
            return null;
        }

        var existingFunction = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);

        if (string.IsNullOrEmpty(existingFunction))
        {
            return null;
        }

        await reader.NextResultAsync(ct).ConfigureAwait(false);
        var drops = new List<string>();
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            drops.Add(await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false));
        }

        return new Function(Identifier, existingFunction.TrimEnd() + ";", drops.ToArray());
    }

    public string Body(Migrator? rules = null)
    {
        rules ??= new PostgresqlMigrator();
        var writer = new StringWriter();
        WriteCreateStatement(rules, writer);

        return writer.ToString();
    }

    public string[] DropStatements()
    {
        if (_dropStatements?.Length > 0)
        {
            return _dropStatements;
        }

        if (IsRemoved)
        {
            return Array.Empty<string>();
        }

        var signature = ParseSignature(Body());

        var drop = $"drop function if exists {signature};";

        return new[] { drop };
    }


    public static Function ForSql(string sql)
    {
        var identifier = ParseIdentifier(sql);
        return new Function(identifier, sql);
    }

    public async Task<FunctionDelta> FindDeltaAsync(NpgsqlConnection conn, CancellationToken ct = default)
    {
        var existing = await FetchExistingAsync(conn, ct).ConfigureAwait(false);
        return new FunctionDelta(this, existing);
    }

    public static Function ForRemoval(string identifier)
    {
        return ForRemoval(DbObjectName.Parse(PostgresqlProvider.Instance, identifier));
    }

    public static Function ForRemoval(DbObjectName identifier)
    {
        return new Function(identifier, null) { IsRemoved = true };
    }

    public string BuildTemplate(string template)
    {
        var body = Body();
        var signature = ParseSignature(body);

        return template
            .Replace(Migrator.SCHEMA, Identifier.Schema)
            .Replace(Migrator.FUNCTION, Identifier.Name)
            .Replace(Migrator.SIGNATURE, signature);
    }

    public void WriteTemplate(Migrator rules, SqlTemplate template, TextWriter writer)
    {
        var text = template?.FunctionCreation;
        if (text.IsNotEmpty())
        {
            writer.WriteLine(BuildTemplate(text));
        }
    }
}
