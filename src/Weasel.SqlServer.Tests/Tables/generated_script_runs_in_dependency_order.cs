using JasperFx;
using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core;
using Weasel.Core.Migrations;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     weasel#677, the SQL Server side. See the PostgreSQL class of the same name for why only
///     executing the script proves anything.
/// </summary>
[Collection("integration")]
public class generated_script_runs_in_dependency_order: IntegrationContext
{
    public generated_script_runs_in_dependency_order(): base("scriptorder")
    {
    }

    private static Table table(string name, params string[] references)
    {
        var table = new Table(new SqlServerObjectName("scriptorder", name));
        table.AddColumn<int>("id").AsPrimaryKey();

        foreach (var reference in references)
        {
            table.AddColumn<int>($"{reference}_id").AllowNulls();
            table.ForeignKeys.Add(new ForeignKey($"fk_{name}_to_{reference}")
            {
                LinkedTable = new SqlServerObjectName("scriptorder", reference),
                ColumnNames = [$"{reference}_id"],
                LinkedNames = ["id"]
            });
        }

        return table;
    }

    /// <summary>
    ///     Through <see cref="SchemaObjectsExtensions.RunSqlAsync" /> rather than a raw command, so
    ///     the <c>GO</c> batch separators a rendered SQL Server script carries are split the way
    ///     Weasel's own executors split them (weasel#593).
    /// </summary>
    private Task executeAsync(string script) => theConnection.RunSqlAsync(script);

    private async Task<int> constraintCountAsync()
    {
        var count = await theConnection.CreateCommand(
                """
                select count(*) from sys.foreign_keys fk
                join sys.schemas s on s.schema_id = fk.schema_id
                where s.name = 'scriptorder'
                """)
            .ExecuteScalarAsync();

        return Convert.ToInt32(count);
    }

    [Fact]
    public async Task a_child_yielded_before_its_parent_still_runs()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("scriptorder", ConnectionSource.ConnectionString);
        db.AddTable(table("so_child", "so_parent"));
        db.AddTable(table("so_parent"));

        await executeAsync(db.ToDatabaseScript());

        (await constraintCountAsync()).ShouldBe(1);
        await db.AssertDatabaseMatchesConfigurationAsync();
    }

    [Fact]
    public async Task a_chain_yielded_backwards_still_runs()
    {
        await ResetSchema();

        var db = new DatabaseWithTables("scriptorder", ConnectionSource.ConnectionString);
        db.AddTable(table("so_c", "so_b"));
        db.AddTable(table("so_b", "so_a"));
        db.AddTable(table("so_a"));

        await executeAsync(db.ToDatabaseScript());

        (await constraintCountAsync()).ShouldBe(2);
        await db.AssertDatabaseMatchesConfigurationAsync();
    }
}
