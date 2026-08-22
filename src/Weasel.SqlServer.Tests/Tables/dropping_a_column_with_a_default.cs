using JasperFx;
using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     weasel#505. SQL Server refuses <c>ALTER TABLE ... DROP COLUMN</c> while a default constraint
///     still references the column.
/// </summary>
/// <remarks>
///     <para>
///         So a column declared with <c>DefaultValue(...)</c> could be added by a migration but never
///         removed by one — the patch was generated, it just always failed:
///     </para>
///     <code>
///     The object 'DF__orders__stamp__384F51F2' is dependent on column 'stamp'.
///     ALTER TABLE DROP COLUMN stamp failed because one or more objects access this column.
///     </code>
///     <para>
///         PostgreSQL drops a column and its default together, so this is SQL-Server-only. The awkward
///         part is that SQL Server names the constraint itself when the DDL does not
///         (<c>DF__table__col__hash</c>), so the drop cannot be written statically — it has to look the
///         name up at run time.
///     </para>
///     <para>
///         Reported from Wolverine, where <c>DurabilitySettings.OutboxStaleTime</c> adds a defaulted
///         timestamp column to the envelope tables: turning the option back off left a database that
///         could never be migrated forward again (JasperFx/wolverine#3997).
///     </para>
/// </remarks>
public class dropping_a_column_with_a_default: IntegrationContext
{
    // The fixture keeps its schema name private, so hold it here rather than reaching for it.
    private const string Schema = "dropdefault";

    public dropping_a_column_with_a_default(): base(Schema)
    {
    }

    private Table BuildTable(bool withDefaultedColumn, bool withSecondDefaultedColumn = false)
    {
        var table = new Table(new SqlServerObjectName(Schema, "orders"));
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").AllowNulls();

        if (withDefaultedColumn)
        {
            table.AddColumn<DateTime>("stamp").DefaultValueByExpression("GETUTCDATE()");
        }

        if (withSecondDefaultedColumn)
        {
            table.AddColumn<DateTime>("stamp2").DefaultValueByExpression("GETUTCDATE()");
        }

        return table;
    }

    private async Task<int> ColumnCountAsync(string column)
    {
        var result = await theConnection.CreateCommand(
                "select count(*) from information_schema.columns "
                + $"where table_schema = '{Schema}' and table_name = 'orders' and column_name = '{column}'")
            .ExecuteScalarAsync();

        return Convert.ToInt32(result);
    }

    private async Task<int> DefaultConstraintCountAsync()
    {
        var result = await theConnection.CreateCommand(
                "select count(*) from sys.default_constraints dc "
                + $"where dc.parent_object_id = object_id('{Schema}.orders')")
            .ExecuteScalarAsync();

        return Convert.ToInt32(result);
    }

    [Fact]
    public async Task the_defaulted_column_can_be_dropped()
    {
        await ResetSchema();

        await new SqlServerMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable(withDefaultedColumn: true)),
            AutoCreate.CreateOrUpdate);

        (await ColumnCountAsync("stamp")).ShouldBe(1);

        var without = BuildTable(withDefaultedColumn: false);
        var migration = await SchemaMigration.DetermineAsync(theConnection, without);
        migration.Difference.ShouldBe(SchemaPatchDifference.Update);

        // Pre-fix this threw: "The object 'DF__orders__stamp__...' is dependent on column 'stamp'."
        await new SqlServerMigrator().ApplyAllAsync(theConnection, migration, AutoCreate.CreateOrUpdate);

        (await ColumnCountAsync("stamp")).ShouldBe(0);

        // The constraint goes with it rather than being orphaned.
        (await DefaultConstraintCountAsync()).ShouldBe(0);

        (await SchemaMigration.DetermineAsync(theConnection, without)).Difference
            .ShouldBe(SchemaPatchDifference.None);
    }

    /// <summary>
    ///     Two defaulted columns dropped in one migration. A SQL Server delta is executed as a single
    ///     command per table, so a lookup that declared a variable at batch scope would fail here with
    ///     "The variable name '@...' has already been declared."
    /// </summary>
    [Fact]
    public async Task two_defaulted_columns_can_be_dropped_in_one_migration()
    {
        await ResetSchema();

        await new SqlServerMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection,
                BuildTable(withDefaultedColumn: true, withSecondDefaultedColumn: true)),
            AutoCreate.CreateOrUpdate);

        (await DefaultConstraintCountAsync()).ShouldBe(2);

        var without = BuildTable(withDefaultedColumn: false);
        await new SqlServerMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, without), AutoCreate.CreateOrUpdate);

        (await ColumnCountAsync("stamp")).ShouldBe(0);
        (await ColumnCountAsync("stamp2")).ShouldBe(0);
        (await DefaultConstraintCountAsync()).ShouldBe(0);
    }

    /// <summary>
    ///     A column with no default still drops, and the lookup finding nothing is not an error.
    /// </summary>
    [Fact]
    public async Task an_undefaulted_column_still_drops()
    {
        await ResetSchema();

        await new SqlServerMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, BuildTable(withDefaultedColumn: false)),
            AutoCreate.CreateOrUpdate);

        var without = new Table(new SqlServerObjectName(Schema, "orders"));
        without.AddColumn<int>("id").AsPrimaryKey();

        await new SqlServerMigrator().ApplyAllAsync(theConnection,
            await SchemaMigration.DetermineAsync(theConnection, without), AutoCreate.CreateOrUpdate);

        (await ColumnCountAsync("name")).ShouldBe(0);
    }
}
