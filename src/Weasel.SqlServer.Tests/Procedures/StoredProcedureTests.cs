using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Procedures;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Procedures;

internal class OutgoingEnvelopeTable: Table
{
    public static readonly string TableName = "jasper_outgoing_envelopes";

    public OutgoingEnvelopeTable(string schemaName): base(new DbObjectName(schemaName, TableName))
    {
        AddColumn<Guid>("id").AsPrimaryKey();
        AddColumn<int>("owner_id").NotNull();
        AddColumn<string>("destination").NotNull();
        AddColumn<DateTimeOffset>("deliver_by");
        AddColumn("body", "binary").NotNull();
    }
}

internal class IncomingEnvelopeTable: Table
{
    public static readonly string TableName = "jasper_incoming_envelopes";

    public IncomingEnvelopeTable(string schemaName): base(new DbObjectName(schemaName, TableName))
    {
        AddColumn<Guid>("id").AsPrimaryKey();
        AddColumn<string>("status").NotNull();
        AddColumn<int>("owner_id").NotNull();
        AddColumn<DateTimeOffset>("execution_time").DefaultValueByExpression("NULL");
        AddColumn<int>("attempts");
        AddColumn("body", "binary").NotNull();
    }
}

internal class DeadLettersTable: Table
{
    public static readonly string TableName = "jasper_dead_letters";

    public DeadLettersTable(string schemaName): base(new DbObjectName(schemaName, TableName))
    {
        AddColumn<Guid>("id").AsPrimaryKey();
        AddColumn<string>("source");
        AddColumn<string>("message_type");
        AddColumn<string>("explanation");
        AddColumn<string>("exception_text");
        AddColumn<string>("exception_type");
        AddColumn<string>("exception_message");
        AddColumn("body", "binary").NotNull();
    }
}

public class StoredProcedureTests: IntegrationContext
{
    private StoredProcedure theProcedure;

    public StoredProcedureTests(): base("procs")
    {
        theProcedure = new StoredProcedure(new DbObjectName("procs", "uspDeleteIncomingEnvelopes"), @"
CREATE PROCEDURE procs.uspDeleteIncomingEnvelopes
    @IDLIST procs.EnvelopeIdList READONLY
AS

    DELETE FROM procs.jasper_incoming_envelopes WHERE id IN (SELECT ID FROM @IDLIST);
");
    }

    public override async Task InitializeAsync()
    {
        await ResetSchema();

        var table = new IncomingEnvelopeTable("procs");
        await table.CreateAsync(theConnection);

        var type = new TableType(new DbObjectName("procs", "EnvelopeIdList"));
        type.AddColumn<Guid>("ID");

        await type.ApplyChangesAsync(theConnection);
    }

    private void afterChangingTheProcedure()
    {
        theProcedure = new StoredProcedure(new DbObjectName("procs", "uspDeleteIncomingEnvelopes"), @"
CREATE PROCEDURE procs.uspDeleteIncomingEnvelopes
    @IDLIST procs.EnvelopeIdList READONLY
AS

    UPDATE procs.jasper_incoming_envelopes SET status = 'ok' WHERE id IN (SELECT ID FROM @IDLIST);
");
    }


    [Fact]
    public async Task can_create_a_function()
    {
        await theProcedure.CreateAsync(theConnection);
    }

    [Fact]
    public async Task fetch_existing()
    {
        await theProcedure.CreateAsync(theConnection);

        var existing = await theProcedure.FetchExistingAsync(theConnection);
        existing.ShouldNotBeNull();
    }


    [Fact]
    public async Task fetch_existing_when_it_does_not_exist()
    {
        var existing = await theProcedure.FetchExistingAsync(theConnection);
        existing.ShouldBeNull();
    }

    [Fact]
    public async Task fetch_delta_when_does_not_exist()
    {
        var delta = await theProcedure.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }


    [Fact]
    public async Task apply_new_delta()
    {
        await theProcedure.CreateAsync(theConnection);

        afterChangingTheProcedure();

        await theProcedure.ApplyChangesAsync(theConnection);

        var delta = await theProcedure.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }


    [Fact]
    public async Task fetch_delta_with_different_body()
    {
        await theProcedure.CreateAsync(theConnection);

        afterChangingTheProcedure();

        var delta = await theProcedure.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Update);
    }

    [Fact]
    public async Task apply_update_delta()
    {
        await theProcedure.CreateAsync(theConnection);

        afterChangingTheProcedure();

        await theProcedure.ApplyChangesAsync(theConnection);

        var delta = await theProcedure.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }


    [Fact]
    public async Task fetch_delta_with_no_differences()
    {
        await theProcedure.CreateAsync(theConnection);

        var delta = await theProcedure.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task drop_procedure()
    {
        await theProcedure.CreateAsync(theConnection);

        await theProcedure.Drop(theConnection);

        var delta = await theProcedure.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }
}
