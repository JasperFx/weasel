using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Procedures;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Triggers;
using Weasel.Postgresql.Types;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     SchemaMigration concatenates every object's introspection query into one command, so each
///     query has to terminate itself. An object whose query does not is fine alone and breaks the
///     moment anything follows it.
/// </summary>
[Collection("batched")]
public class batched_introspection_queries: IntegrationContext
{
    public batched_introspection_queries(): base("batched")
    {
    }

    private Table aTable()
    {
        var table = new Table("batched.thing");
        table.AddColumn<int>("id").AsPrimaryKey();
        return table;
    }

    [Fact]
    public async Task a_stored_procedure_followed_by_another_object()
    {
        await ResetSchema();

        var proc = new StoredProcedure(new DbObjectName("batched", "do_thing"), @"
create or replace procedure batched.do_thing() language plpgsql as $$
begin
  perform 1;
end;
$$;");

        var migration = await SchemaMigration.DetermineAsync(theConnection, proc, aTable());

        migration.ShouldNotBeNull();
    }

    [Fact]
    public async Task a_user_defined_type_followed_by_another_object()
    {
        await ResetSchema();

        var type = UserDefinedType.Enum("batched.mood", "happy", "sad");

        var migration = await SchemaMigration.DetermineAsync(theConnection, type, aTable());

        migration.ShouldNotBeNull();
    }

    [Fact]
    public async Task a_trigger_followed_by_another_object()
    {
        await ResetSchema();

        var trigger = new Trigger(
            new DbObjectName("batched", "thing_stamp"),
            new DbObjectName("batched", "thing"),
            "before insert on batched.thing for each row execute function batched.stamp()");

        var migration = await SchemaMigration.DetermineAsync(theConnection, trigger, aTable());

        migration.ShouldNotBeNull();
    }
}
