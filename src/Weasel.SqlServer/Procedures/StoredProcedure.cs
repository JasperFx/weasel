using System.Data.Common;
using JasperFx.Core;
using Microsoft.Data.SqlClient;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.SqlServer.Procedures;

public class StoredProcedure: ISchemaObject
{
    private readonly string? _body;

    public StoredProcedure(DbObjectName identifier)
    {
        Identifier = identifier;
    }

    public StoredProcedure(DbObjectName identifier, string body)
    {
        Identifier = identifier;
        _body = body;
    }

    public bool IsRemoved { get; set; }

    public void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        if (_body.IsNotEmpty())
        {
            writer.WriteLine(_body);
        }
        else
        {
            generateBody(writer);
        }
    }

    public void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        writer.WriteLine($"drop procedure if exists {Identifier};");
    }

    public void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        builder.Append($@"
select
    sys.sql_modules.definition
from sys.sql_modules
inner join sys.objects on sys.sql_modules.object_id = sys.objects.object_id
inner join sys.schemas on sys.objects.schema_id = sys.schemas.schema_id
where
    sys.objects.name = '{Identifier.Name}' and
    sys.schemas.name = '{Identifier.Schema}'
");
    }

    public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
    {
        var existing = await readExisting(reader).ConfigureAwait(false);
        return new StoredProcedureDelta(this, existing);
    }

    public IEnumerable<DbObjectName> AllNames()
    {
        yield return Identifier;
    }

    public DbObjectName Identifier { get; }

    protected virtual string generateBody(TextWriter writer)
    {
        throw new NotSupportedException(
            "This must be implemented in subclasses that do not inject the procedure body");
    }

    public void WriteCreateOrAlterStatement(Migrator rules, TextWriter writer)
    {
        var body = _body;
        if (_body.IsEmpty())
        {
            var w = new StringWriter();
            generateBody(w);

            body = w.ToString();
        }

        body = body!.Replace("CREATE PROCEDURE", "CREATE OR ALTER PROCEDURE");
        body = body.Replace("create procedure", "create or alter procedure");

        writer.WriteLine(body);
    }

    private async Task<StoredProcedure?> readExisting(DbDataReader reader)
    {
        if (await reader.ReadAsync().ConfigureAwait(false))
        {
            var body = await reader.GetFieldValueAsync<string>(0).ConfigureAwait(false);
            return new StoredProcedure(Identifier, body);
        }

        return null;
    }

    public string CanonicizeSql()
    {
        var body = _body;
        if (_body.IsEmpty())
        {
            var writer = new StringWriter();
            generateBody(writer);

            body = writer.ToString();
        }

        return body!.ReadLines().Select(x => x.Trim()).Where(x => x.IsNotEmpty())
            .Select(x => x.Replace("   ", " ")).Join(Environment.NewLine);
    }

    public async Task<StoredProcedure?> FetchExisting(SqlConnection conn)
    {
        var builder = new DbCommandBuilder(conn);

        ConfigureQueryCommand(builder);

        await using var reader = await builder.ExecuteReaderAsync(conn).ConfigureAwait(false);
        return await readExisting(reader).ConfigureAwait(false);
    }


    public async Task<StoredProcedureDelta> FindDelta(SqlConnection conn)
    {
        var actual = await FetchExisting(conn).ConfigureAwait(false);
        return new StoredProcedureDelta(this, actual);
    }
}
