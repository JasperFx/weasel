using System.Data.Common;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer.Functions;

public class Function: FunctionBase
{
    public Function(DbObjectName identifier, string body, string[] dropStatements)
        : base(identifier, body, dropStatements)
    {
    }

    public Function(DbObjectName identifier, string? body) : base(identifier, body)
    {
    }

    protected Function(DbObjectName identifier) : base(identifier, body: null)
    {
    }

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine($"EXEC sp_executesql N'{RawBody}';");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var nameParam = builder.AddParameter(Identifier.ToString()).ParameterName;
        builder.Append($"SELECT sm.definition FROM sys.sql_modules AS sm WHERE sm.object_id = OBJECT_ID(@{nameParam})");
    }

    protected override Migrator GetDefaultMigrator() => new SqlServerMigrator();

    protected override string[] ComputeDefaultDropStatements()
    {
        var drop = $"drop function if exists {Identifier};";
        return new[] { drop };
    }

    protected override async Task<FunctionBase?> ReadExistingFromReaderAsync(
        DbDataReader reader, CancellationToken ct)
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

    protected override ISchemaObjectDelta CreateFunctionDelta(FunctionBase? actual)
        => new FunctionDelta(this, (Function?)actual);

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
        var result = await ReadExistingFromReaderAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);
        return (Function?)result;
    }

    public static Task<Function?> FetchExistingAsync(SqlConnection conn, DbObjectName identifier, CancellationToken ct = default)
    {
        var function = new Function(identifier);
        return function.FetchExistingAsync(conn, ct);
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
