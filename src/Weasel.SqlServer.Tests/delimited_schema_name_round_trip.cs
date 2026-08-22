using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     weasel#499 on SQL Server, where the same root cause surfaces loudly instead of silently.
/// </summary>
/// <remarks>
///     <para>
///         <c>SqlServerMigrator.CreateSchemaStatementFor</c> guards on
///         <c>sys.schemas where name = @schema</c>. A bracketed spelling misses that guard the same
///         way it misses <c>information_schema</c>, so the migration attempted <c>CREATE SCHEMA</c>
///         for a schema that had been there all along and the whole script died with
///         <c>There is already an object named 'x' in the database</c>.
///     </para>
///     <para>
///         This is the SQL Server twin of the PostgreSQL guard bugs in weasel#495 and weasel#498 —
///         same shape, same cause, and it falls out of normalizing the name on the way into the model.
///     </para>
/// </remarks>
public class delimited_schema_name_round_trip: IntegrationContext
{
    public delimited_schema_name_round_trip(): base("bracketed")
    {
    }

    private static Table Build(bool withSecondColumn)
    {
        var table = new Table(DbObjectName.Parse(SqlServerProvider.Instance, "[bracketed].things"));
        table.AddColumn<int>("id").AsPrimaryKey();
        if (withSecondColumn)
        {
            table.AddColumn<string>("added_later").AllowNulls();
        }

        return table;
    }

    [Fact]
    public void the_schema_is_held_without_its_brackets()
    {
        Build(false).Identifier.Schema.ShouldBe("bracketed");
    }

    /// <summary>
    ///     With the schema already present, the migration must not try to create it again.
    /// </summary>
    [Fact]
    public async Task an_existing_schema_is_not_created_a_second_time()
    {
        await ResetSchema();

        // Pre-fix this threw SqlException "There is already an object named 'bracketed' in the
        // database" on the CREATE SCHEMA that opens the script.
        await new SqlServerMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, Build(false)), AutoCreate.CreateOrUpdate);

        (await SchemaMigration.DetermineAsync(theConnection, Build(false)))
            .Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task a_later_change_is_applied_rather_than_silently_discarded()
    {
        await ResetSchema();

        await new SqlServerMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, Build(false)), AutoCreate.CreateOrUpdate);

        var migration = await SchemaMigration.DetermineAsync(theConnection, Build(true));
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        var count = await theConnection.CreateCommand(
                "select count(*) from information_schema.columns where table_schema = 'bracketed' "
                + "and table_name = 'things' and column_name = 'added_later'")
            .ExecuteScalarAsync();

        Convert.ToInt32(count).ShouldBe(1, "the added column never reached the database");
    }
}
