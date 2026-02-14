using JasperFx.Core.Reflection;
using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Tables;
using Xunit;

namespace Weasel.Oracle.Tests.Tables;

public class detecting_table_deltas: IntegrationContext
{
    private Table theTable;

    public detecting_table_deltas(): base("DELTAS")
    {
        theTable = new Table("DELTAS.PEOPLE");
        theTable.AddColumn<int>("id").AsPrimaryKey();
        theTable.AddColumn<string>("first_name");
        theTable.AddColumn<string>("last_name");
        theTable.AddColumn<string>("user_name");
        theTable.AddColumn("data", "CLOB");
    }

    public override async Task InitializeAsync()
    {
        await ResetSchema();
    }

    protected async Task AssertNoDeltasAfterPatching(Table? table = null)
    {
        table ??= theTable;
        try
        {
            await table.ApplyChangesAsync(theConnection);
        }
        catch (OracleException e)
        {
            if (e.Message.Contains("deadlock"))
            {
                await Task.Delay(100);
                await theConnection.CloseAsync();
                await theConnection.OpenAsync();
                await table.ApplyChangesAsync(theConnection);
            }
            else
            {
                throw;
            }
        }

        TableDelta delta;
        try
        {
            delta = await table.FindDeltaAsync(theConnection);
        }
        catch (OracleException e)
        {
            if (e.Message.Contains("deadlock"))
            {
                await Task.Delay(100);
                await theConnection.CloseAsync();
                await theConnection.OpenAsync();
                delta = await table.FindDeltaAsync(theConnection);
            }
            else
            {
                throw;
            }
        }

        if (delta.HasChanges())
        {
            var writer = new StringWriter();
            delta.WriteUpdate(new OracleMigrator(), writer);
            throw new Exception("Found these differences:\n\n" + writer.ToString());
        }

        delta.HasChanges().ShouldBeFalse();
    }

    [Fact]
    public async Task detect_all_new_table()
    {
        var table = await theTable.FetchExistingAsync(theConnection);
        table.ShouldBeNull();

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [Fact]
    public async Task no_delta()
    {
        await CreateSchemaObjectInDatabase(theTable);

        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeFalse();

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task missing_column()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.AddColumn<DateTimeOffset>("birth_day");
        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        delta.Columns.Missing.Single().Name.ShouldBe("birth_day");

        delta.Columns.Extras.Any().ShouldBeFalse();
        delta.Columns.Different.Any().ShouldBeFalse();

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task extra_column()
    {
        theTable.AddColumn<DateTime>("birth_day");
        await CreateSchemaObjectInDatabase(theTable);

        theTable.RemoveColumn("birth_day");

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        delta.Columns.Extras.Single().Name.ShouldBe("birth_day");

        delta.Columns.Missing.Any().ShouldBeFalse();
        delta.Columns.Different.Any().ShouldBeFalse();

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task detect_new_index()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        delta.Indexes.Missing.Single()
            .Name.ShouldBe("idx_PEOPLE_user_name");

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task detect_matched_index()
    {
        theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);

        await CreateSchemaObjectInDatabase(theTable);

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeFalse();

        delta.Indexes.Matched.Single()
            .Name.ShouldBe("IDX_PEOPLE_USER_NAME");

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task detect_different_index()
    {
        theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);

        await CreateSchemaObjectInDatabase(theTable);

        var indexDefinition = theTable.Indexes.Single().As<IndexDefinition>();
        indexDefinition.SortOrder = SortOrder.Desc;
        indexDefinition.IsUnique = false;

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        delta.Indexes.Different.Single()
            .Expected
            .Name.ShouldBe("idx_PEOPLE_user_name");

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task detect_extra_index()
    {
        theTable.ModifyColumn("user_name").AddIndex(i => i.IsUnique = true);
        await CreateSchemaObjectInDatabase(theTable);

        theTable.Indexes.Clear();

        var delta = await theTable.FindDeltaAsync(theConnection);
        delta.HasChanges().ShouldBeTrue();

        delta.Indexes.Extras.Single().Name
            .ShouldBe("IDX_PEOPLE_USER_NAME");

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching();
    }

    [Fact]
    public async Task detect_all_new_foreign_key()
    {
        var states = new Table("DELTAS.STATES");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);

        var table = new Table("DELTAS.PEOPLE2");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        await CreateSchemaObjectInDatabase(table);

        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        var delta = await table.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeTrue();

        delta.ForeignKeys.Missing.Single()
            .ShouldBeSameAs(table.ForeignKeys.Single());

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task detect_extra_foreign_key()
    {
        var states = new Table("DELTAS.STATES2");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);

        var table = new Table("DELTAS.PEOPLE3");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(table);

        table.ForeignKeys.Clear();

        var delta = await table.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeTrue();

        delta.ForeignKeys.Extras.Single().Name
            .ShouldBe("FK_PEOPLE3_STATE_ID");

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task match_foreign_key()
    {
        var states = new Table("DELTAS.STATES3");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);

        var table = new Table("DELTAS.PEOPLE4");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(table);

        var delta = await table.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeFalse();

        delta.ForeignKeys.Matched.Single().Name
            .ShouldBe("FK_PEOPLE4_STATE_ID");

        delta.Difference.ShouldBe(SchemaPatchDifference.None);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task different_foreign_key()
    {
        var states = new Table("DELTAS.STATES4");
        states.AddColumn<int>("id").AsPrimaryKey();

        await CreateSchemaObjectInDatabase(states);

        var table = new Table("DELTAS.PEOPLE5");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("first_name");
        table.AddColumn<string>("last_name");

        table.AddColumn<int>("state_id").ForeignKeyTo(states, "id");

        await CreateSchemaObjectInDatabase(table);

        table.ForeignKeys.Single().OnDelete = CascadeAction.Cascade;

        var delta = await table.FindDeltaAsync(theConnection);

        delta.HasChanges().ShouldBeTrue();

        delta.ForeignKeys.Different.Single().Actual.Name
            .ShouldBe("FK_PEOPLE5_STATE_ID");

        delta.Difference.ShouldBe(SchemaPatchDifference.Update);

        await AssertNoDeltasAfterPatching(table);
    }

    [Fact]
    public async Task detect_primary_key_change()
    {
        await CreateSchemaObjectInDatabase(theTable);

        theTable.AddColumn<string>("tenant_id").AsPrimaryKey().DefaultValueByString("foo");
        var delta = await theTable.FindDeltaAsync(theConnection);

        delta.PrimaryKeyDifference.ShouldBe(SchemaPatchDifference.Update);
        delta.HasChanges().ShouldBeTrue();

        await AssertNoDeltasAfterPatching(theTable);
    }
}
