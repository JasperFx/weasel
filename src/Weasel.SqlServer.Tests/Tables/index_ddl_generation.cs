using Shouldly;
using Weasel.Core;
using Weasel.SqlServer.Tables;
using Xunit;

namespace Weasel.SqlServer.Tests.Tables;

/// <summary>
///     SQL Server's CREATE INDEX grammar fixes the order of both the modifiers and the trailing
///     clauses. Emitting them in any other order is a syntax error, and each of these only shows up
///     on an index that combines more than one feature — which is why they survived.
/// </summary>
public class index_ddl_generation: IntegrationContext
{
    public index_ddl_generation(): base("indexddl")
    {
    }

    private static Table theTable()
    {
        var table = new Table("indexddl.item");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("group_id").NotNull();
        table.AddColumn<int>("deleted").NotNull();
        table.AddColumn<string>("note").AllowNulls();
        return table;
    }

    [Fact]
    public void unique_comes_before_clustered()
    {
        var index = new IndexDefinition("ix_unique_clustered")
        {
            Columns = ["group_id"], IsUnique = true, IsClustered = true
        };

        // "CREATE CLUSTERED UNIQUE INDEX" is rejected by the parser.
        index.ToDDL(theTable()).ShouldStartWith("CREATE UNIQUE CLUSTERED INDEX");
    }

    [Fact]
    public void include_then_where_then_with()
    {
        var index = new IndexDefinition("ix_everything")
        {
            Columns = ["group_id"],
            IncludedColumns = ["note"],
            Predicate = "deleted = 0",
            FillFactor = 90
        };

        var ddl = index.ToDDL(theTable());

        ddl.IndexOf("INCLUDE", StringComparison.Ordinal)
            .ShouldBeLessThan(ddl.IndexOf("WHERE", StringComparison.Ordinal));
        ddl.IndexOf("WHERE", StringComparison.Ordinal)
            .ShouldBeLessThan(ddl.IndexOf("WITH", StringComparison.Ordinal));
    }

    [Fact]
    public async Task an_index_combining_include_where_and_fillfactor_is_accepted_by_the_server()
    {
        await ResetSchema();

        var table = theTable();
        table.Indexes.Add(new IndexDefinition("ix_everything")
        {
            Columns = ["group_id"],
            IncludedColumns = ["note"],
            Predicate = "deleted = 0",
            FillFactor = 90
        });

        // The real proof: SQL Server parses it.
        await CreateSchemaObjectInDatabase(table);

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.IndexFor("ix_everything").ShouldNotBeNull();
    }

    [Fact]
    public async Task a_unique_clustered_index_is_accepted_by_the_server()
    {
        await ResetSchema();

        // The PK has to be nonclustered or the table already has its one clustered index, and the
        // failure would be "cannot create more than one clustered index" rather than the syntax
        // error this test is about.
        await theConnection.CreateCommand(
                "create table indexddl.uniqueclustered (id int not null constraint pk_uc primary key nonclustered, code int not null)")
            .ExecuteNonQueryAsync();

        var table = new Table("indexddl.uniqueclustered");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<int>("code").NotNull();
        var index = new IndexDefinition("ix_code") { Columns = ["code"], IsUnique = true, IsClustered = true };

        index.ToDDL(table).ShouldStartWith("CREATE UNIQUE CLUSTERED INDEX");

        // CREATE CLUSTERED UNIQUE INDEX is a syntax error; the server is the proof.
        await theConnection.CreateCommand(index.ToDDL(table)).ExecuteNonQueryAsync();

        var existing = await table.FetchExistingAsync(theConnection);
        existing!.IndexFor("ix_code")!.IsUnique.ShouldBeTrue();
    }


}
