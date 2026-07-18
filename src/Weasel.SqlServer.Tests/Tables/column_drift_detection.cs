using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     Opt-in column drift detection (ITable.DetectColumnDrift) on SQL Server:
///     default expressions and nullability of otherwise-matching columns
///     participate in delta detection and are corrected — including dropping
///     the server-named default constraint before re-adding the default.
/// </summary>
public class column_drift_detection: IntegrationContext
{
    public column_drift_detection(): base("column_drift")
    {
    }

    public override Task InitializeAsync() => ResetSchema();

    private async Task AssertNoDeltasAfterPatching(Table table)
    {
        var delta = await table.FindDeltaAsync(theConnection);
        var migration = new SchemaMigration(delta);
        var migrator = new SqlServerMigrator();
        await migrator.ApplyAllAsync(theConnection, migration, JasperFx.AutoCreate.CreateOrUpdate);

        var after = await table.FindDeltaAsync(theConnection);
        after.HasChanges().ShouldBeFalse();
    }

    private Table theTable()
    {
        var table = new Table("column_drift.people");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("status");
        table.ColumnFor("status")!.DefaultExpression = "'pending'";
        table.ColumnFor("status")!.AllowNulls = false;
        table.AddColumn<int>("score");
        table.ColumnFor("score")!.DefaultExpression = "42";
        table.DetectColumnDrift = true;
        return table;
    }

    [Fact]
    public async Task no_drift_is_no_delta()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task detects_and_corrects_changed_default()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        await theConnection.CreateCommand("""
            declare @dc nvarchar(max);
            select @dc = dc.name from sys.default_constraints dc
                inner join sys.columns c on c.default_object_id = dc.object_id
                where dc.parent_object_id = OBJECT_ID('column_drift.people') and c.name = 'score';
            if @dc is not null exec('alter table column_drift.people drop constraint ' + @dc);
            alter table column_drift.people add default 99 for score;
            """).ExecuteNonQueryAsync();

        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task detects_and_corrects_nullability_drift()
    {
        var table = theTable();
        await CreateSchemaObjectInDatabase(table);

        await theConnection.CreateCommand("alter table column_drift.people alter column status varchar(100) null;")
            .ExecuteNonQueryAsync();

        var delta = await table.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }
}
