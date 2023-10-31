using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

public class TableTypeTests: IntegrationContext
{
    public TableTypeTests(): base("table_types")
    {
    }

    [Fact]
    public async Task create_table_type()
    {
        await ResetSchema();

        var type = new TableType(new SqlServerObjectName("table_types", "EnvelopeIdList"));
        type.AddColumn<Guid>("ID");

        await type.CreateAsync(theConnection);
    }

    [Fact]
    public async Task fetch_existing_when_it_does_not_exist()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");

        var existing = await type.FetchExistingAsync(theConnection);
        existing.ShouldBeNull();
    }

    [Fact]
    public async Task fetch_existing_when_it_does_exist()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");

        await type.CreateAsync(theConnection);

        var existing = await type.FetchExistingAsync(theConnection);
        existing.ShouldNotBeNull();
        existing.Columns.Count.ShouldBe(1);
        existing.Columns[0].Name.ShouldBe("ID");
    }

    [Fact]
    public async Task fetch_delta_when_does_not_exist()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");

        var delta = await type.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }


    [Fact]
    public async Task apply_new_delta()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");

        await type.ApplyChangesAsync(theConnection);

        var delta = await type.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }


    [Fact]
    public async Task fetch_delta_with_no_differences()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");

        await type.CreateAsync(theConnection);

        var delta = await type.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task fetch_delta_with_no_differences_2()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");
        type.AddColumn("name", "varchar");

        await type.CreateAsync(theConnection);

        var delta = await type.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }


    [Fact]
    public async Task drop_table_type()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");

        await type.CreateAsync(theConnection);

        await type.Drop(theConnection);

        var delta = await type.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }


    [Fact]
    public async Task fetch_delta_with_different_columns()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");

        await type.CreateAsync(theConnection);

        type.Columns[0].DatabaseType = "varchar";

        var delta = await type.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }


    [Fact]
    public async Task fetch_delta_with_different_columns_2()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");

        await type.CreateAsync(theConnection);

        type.Columns[0].AllowNulls = !type.Columns[0].AllowNulls;

        var delta = await type.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }


    [Fact]
    public async Task fetch_delta_with_different_columns_3()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");


        await type.CreateAsync(theConnection);

        type.AddColumn<string>("name");

        var delta = await type.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }


    [Fact]
    public async Task apply_update_delta()
    {
        await ResetSchema();

        var dbObjectName = new SqlServerObjectName("table_types", "EnvelopeIdList");
        var type = new TableType(dbObjectName);
        type.AddColumn<Guid>("ID");


        await type.CreateAsync(theConnection);

        type.AddColumn("name", "varchar");


        await type.ApplyChangesAsync(theConnection);

        var delta = await type.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }
}
