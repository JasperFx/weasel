using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer.Functions;

public class Function: ISchemaObject
{
    private readonly string? _body;
    private readonly string[]? _dropStatements;


    public Function(DbObjectName identifier, string body, string[] dropStatements)
    {
        _body = body;
        _dropStatements = dropStatements;
        Identifier = identifier;
    }

    public Function(DbObjectName identifier, string? body)
    {
        _body = body;
        Identifier = identifier;
    }

    protected Function(DbObjectName identifier)
    {
        Identifier = identifier;
    }


    public bool IsRemoved { get; protected set; }


    public virtual void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine($"EXEC sp_executesql N'{_body}';");
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        foreach (var dropStatement in DropStatements()) writer.WriteLine(dropStatement);
    }

    public DbObjectName Identifier { get; }

    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var nameParam = builder.AddParameter(Identifier.ToString()).ParameterName;
        builder.Append($"SELECT sm.definition FROM sys.sql_modules AS sm WHERE sm.object_id = OBJECT_ID(@{nameParam})");
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

    public static DbObjectName ParseIdentifier(string functionSql)
    {
        var functionIndex = functionSql.IndexOf("FUNCTION", StringComparison.OrdinalIgnoreCase);
        var openParen = functionSql.IndexOf('(');
        var nameStart = functionIndex + "function".Length;
        var funcName = functionSql.Substring(nameStart, openParen - nameStart).Trim();
        return DbObjectName.Parse(SqlServerProvider.Instance, funcName);
    }

    public async Task<Function?> FetchExistingAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);

        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        return await readExistingAsync(reader, ct).ConfigureAwait(false);
    }

    public static Task<Function?> FetchExistingAsync(SqlConnection conn, DbObjectName identifier, CancellationToken ct = default)
    {
        var function = new Function(identifier);
        return function.FetchExistingAsync(conn, ct);
    }

    private async Task<Function?> readExistingAsync(DbDataReader reader, CancellationToken ct = default)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            return null;
        }

        var existingFunction = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        if (string.IsNullOrEmpty(existingFunction))
        {
            return null;
        }

        return new Function(Identifier, existingFunction.TrimEnd());
    }

    public string Body(Migrator? rules = null)
    {
        rules ??= new SqlServerMigrator();
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

        var drop = $"drop function if exists {Identifier};";
        return new[] { drop };
    }


    public static Function ForSql(string sql)
    {
        var identifier = ParseIdentifier(sql);
        return new Function(identifier, sql);
    }

    public async Task<FunctionDelta> FindDeltaAsync(SqlConnection conn, CancellationToken ct = default)
    {
        var existing = await FetchExistingAsync(conn, ct).ConfigureAwait(false);
        return new FunctionDelta(this, existing);
    }

    public static Function ForRemoval(string identifier)
    {
        return ForRemoval(DbObjectName.Parse(SqlServerProvider.Instance, identifier));
    }

    public static Function ForRemoval(DbObjectName identifier)
    {
        return new Function(identifier, null) { IsRemoved = true };
    }
}
