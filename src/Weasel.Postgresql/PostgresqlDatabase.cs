using Npgsql;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.Postgresql.Functions;
using Weasel.Postgresql.Tables;

namespace Weasel.Postgresql;

public abstract class PostgresqlDatabase: DatabaseBase<NpgsqlConnection>, IAsyncDisposable
{
    protected PostgresqlDatabase(
        IMigrationLogger logger,
        AutoCreate autoCreate,
        Migrator migrator,
        string identifier,
        NpgsqlDataSource dataSource
    ): base(logger, autoCreate, migrator, identifier, () => dataSource.CreateConnection())
    {
        DataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    }

    public NpgsqlDataSource DataSource { get; }

    protected override ISchemaObject[] possiblyCheckForSchemas(ISchemaObject[] objects)
    {
        return SchemaExistenceCheck.WithSchemaCheck(objects);
    }

    public async Task<Function?> DefinitionForFunction(DbObjectName function, CancellationToken ct = default)
    {
        await using var conn = CreateConnection();
        await conn.OpenAsync(ct).ConfigureAwait(false);

        return await conn.FindExistingFunction(function, ct: ct).ConfigureAwait(false);
    }

    /// <summary>
    ///     Fetch a list of the existing tables in the database
    /// </summary>
    /// <param name="database"></param>
    /// <returns></returns>
    public async Task<IReadOnlyList<DbObjectName>> SchemaTables(CancellationToken ct = default)
    {
        var schemaNames = AllSchemaNames();

        await using var conn = CreateConnection();

        await conn.OpenAsync(ct).ConfigureAwait(false);

        return await conn.ExistingTablesAsync(schemas: schemaNames, ct: ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Creates a connection from the underlying <see cref="NpgsqlDataSource"/>
    /// </summary>
    /// <param name="targetSessionAttributes">Sets the target session attributes. Only relevant when <see cref="NpgsqlMultiHostDataSource"/> is being used.</param>
    /// <returns></returns>
    public NpgsqlConnection CreateConnection(TargetSessionAttributes targetSessionAttributes = TargetSessionAttributes.Primary)
    {
        if (DataSource is NpgsqlMultiHostDataSource multiHostDataSource)
        {
            return multiHostDataSource.CreateConnection(targetSessionAttributes);
        }

        return base.CreateConnection();
    }

    public override NpgsqlConnection CreateConnection()
    {
        return CreateConnection();
    }

    public ValueTask DisposeAsync()
    {
        return DataSource.DisposeAsync();
    }

    public async Task<Table[]> FetchExistingTablesAsync()
    {
        var tableNames = AllObjects().OfType<Table>().Select(x => x.Identifier).ToArray();

        var list = new List<Table>();
        await using var conn = CreateConnection();
        await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        foreach (var tableName in tableNames)
        {
            var table = await new Table(tableName).FetchExistingAsync(conn, CancellationToken.None).ConfigureAwait(false);
            if (table != null)
            {
                list.Add(table);
            }
        }

        return list.ToArray();
    }
}
