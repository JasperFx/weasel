using Shouldly;
using Weasel.Postgresql;
using Weasel.Sqlite;
using Weasel.SqlServer;
using Weasel.Storage.Flattened;
using Xunit;

namespace Weasel.Core.Tests.Flattened;

/// <summary>
///     What each column map renders to, per dialect shape.
/// </summary>
/// <remarks>
///     <para>
///         This is the load-bearing half of the lift and the half that fails silently. An
///         <c>Increment</c> whose update assignment reads the <em>would-be-inserted</em> value rather
///         than the pre-update row is a self-assignment: valid SQL, no error, and a counter that
///         never moves past its first event. The three spellings of "the row as it stands" —
///         <c>target.c</c> inside a <c>MERGE</c>, an unqualified <c>c</c> inside SQLite's
///         <c>DO UPDATE</c> (where <c>excluded.c</c> would be the would-be-inserted value instead),
///         and <c>tbl.c</c> inside PostgreSQL's — are what these tests pin.
///     </para>
///     <para>
///         Every expectation here is the SQL the corresponding store emits today, so a fragment that
///         changes shape at adoption shows up as a failure rather than as a behaviour difference
///         nobody looked for. Note that only SQL Server's brackets are unconditional: Weasel's SQLite
///         and PostgreSQL identifier rules quote a name only when it has to be quoted, so the
///         ordinary expectations below are bare and the last two tests are where quoting appears.
///     </para>
/// </remarks>
public class flat_table_column_map_rendering
{
    private static readonly DbObjectName SqlServerTable = new SqlServerObjectName("dbo", "flat");
    private static readonly DbObjectName SqliteTable = new SqliteObjectName("main", "flat");
    private static readonly DbObjectName PostgresqlTable = new PostgresqlObjectName("public", "flat");

    private static FlatTableColumnContext SqlServer =>
        new(ReferenceFlatTableDialects.SqlServer, SqlServerTable);

    private static FlatTableColumnContext Sqlite =>
        new(ReferenceFlatTableDialects.Sqlite, SqliteTable);

    private static FlatTableColumnContext Postgresql =>
        new(ReferenceFlatTableDialects.Postgresql, PostgresqlTable);

    [Fact]
    public void member_map_assigns_the_parameter()
    {
        var map = new MemberMap("amount", typeof(decimal));

        map.UpdateExpression(SqlServer, "@p1").ShouldBe("[amount] = @p1");
        map.UpdateExpression(Sqlite, "@p1").ShouldBe("amount = @p1");
        map.UpdateExpression(Postgresql, "p_amount").ShouldBe("amount = p_amount");

        map.InsertExpression(SqlServer, "@p1").ShouldBe("@p1");
        map.InsertExpression(Sqlite, "@p1").ShouldBe("@p1");
        map.InsertExpression(Postgresql, "p_amount").ShouldBe("p_amount");
    }

    [Fact]
    public void incrementing_by_a_member_reads_the_pre_update_row()
    {
        var map = new IncrementMemberMap("amount", typeof(int));

        map.UpdateExpression(SqlServer, "@p1").ShouldBe("[amount] = target.[amount] + @p1");
        map.UpdateExpression(Sqlite, "@p1").ShouldBe("amount = amount + @p1");
        map.UpdateExpression(Postgresql, "p_amount").ShouldBe("amount = flat.amount + p_amount");
    }

    [Fact]
    public void incrementing_by_a_member_inserts_the_value_itself()
    {
        // A row that does not exist yet starts at the increment, not at zero-plus-it.
        var map = new IncrementMemberMap("amount", typeof(int));

        map.InsertExpression(SqlServer, "@p1").ShouldBe("@p1");
        map.InsertExpression(Sqlite, "@p1").ShouldBe("@p1");
        map.InsertExpression(Postgresql, "p_amount").ShouldBe("p_amount");
    }

    [Fact]
    public void decrementing_by_a_member_reads_the_pre_update_row()
    {
        var map = new DecrementMemberMap("amount", typeof(int));

        map.UpdateExpression(SqlServer, "@p1").ShouldBe("[amount] = target.[amount] - @p1");
        map.UpdateExpression(Sqlite, "@p1").ShouldBe("amount = amount - @p1");
        map.UpdateExpression(Postgresql, "p_amount").ShouldBe("amount = flat.amount - p_amount");
    }

    [Fact]
    public void decrementing_by_a_member_inserts_the_negated_value()
    {
        // jasperfx#773: a row that does not exist yet is decremented from zero, so a first sighting
        // of 5 lands at -5. Inserting the parameter as given — which Polecat, Fisher and the first
        // cut of these maps all did — makes a *decrement* event raise the column, which is the one
        // reading of the insert branch nobody could defend.
        var map = new DecrementMemberMap("amount", typeof(int));

        map.InsertExpression(SqlServer, "@p1").ShouldBe("-@p1");
        map.InsertExpression(Sqlite, "@p1").ShouldBe("-@p1");
        map.InsertExpression(Postgresql, "p_amount").ShouldBe("-p_amount");
    }

    [Fact]
    public void the_member_valued_insert_branch_is_deliberately_asymmetric()
    {
        // The two halves of the ruling, side by side so neither can be "tidied up" into the other.
        // Increment inserts the bare parameter and Decrement inserts its negation: both are "apply
        // this event to an implicit zero row", which is the only reading under which they agree.
        var increment = new IncrementMemberMap("amount", typeof(int));
        var decrement = new DecrementMemberMap("amount", typeof(int));

        increment.InsertExpression(Sqlite, "@p1").ShouldBe("@p1");
        decrement.InsertExpression(Sqlite, "@p1").ShouldBe("-@p1");

        decrement.InsertExpression(Sqlite, "@p1")
            .ShouldNotBe(increment.InsertExpression(Sqlite, "@p1"));
    }

    [Fact]
    public void increment_by_one_needs_no_parameter()
    {
        var map = new IncrementMap("count");

        map.RequiresInput.ShouldBeFalse();
        map.ColumnType.ShouldBe(typeof(int));

        map.UpdateExpression(SqlServer, string.Empty).ShouldBe("[count] = target.[count] + 1");
        map.UpdateExpression(Sqlite, string.Empty).ShouldBe("count = count + 1");
        map.UpdateExpression(Postgresql, string.Empty).ShouldBe("count = flat.count + 1");

        map.InsertExpression(SqlServer, string.Empty).ShouldBe("1");
    }

    [Fact]
    public void decrement_by_one_needs_no_parameter()
    {
        var map = new DecrementMap("count");

        map.RequiresInput.ShouldBeFalse();

        map.UpdateExpression(SqlServer, string.Empty).ShouldBe("[count] = target.[count] - 1");
        map.UpdateExpression(Sqlite, string.Empty).ShouldBe("count = count - 1");
        map.UpdateExpression(Postgresql, string.Empty).ShouldBe("count = flat.count - 1");

        map.InsertExpression(SqlServer, string.Empty).ShouldBe("0");
    }

    [Fact]
    public void a_set_string_value_is_a_literal_and_is_escaped()
    {
        // The value is fixed at configuration time, so it is baked in rather than parameterized —
        // which makes escaping mandatory. An undoubled quote closes the literal and the remainder of
        // the value is parsed as SQL (polecat#390).
        var map = new SetStringValueMap("status", "O'Brien");

        map.RequiresInput.ShouldBeFalse();

        map.UpdateExpression(SqlServer, string.Empty).ShouldBe("[status] = 'O''Brien'");
        map.UpdateExpression(Sqlite, string.Empty).ShouldBe("status = 'O''Brien'");
        map.UpdateExpression(Postgresql, string.Empty).ShouldBe("status = 'O''Brien'");

        map.InsertExpression(Sqlite, string.Empty).ShouldBe("'O''Brien'");
    }

    [Fact]
    public void a_set_int_value_is_a_literal()
    {
        var map = new SetIntValueMap("status", -5);

        map.UpdateExpression(SqlServer, string.Empty).ShouldBe("[status] = -5");
        map.InsertExpression(SqlServer, string.Empty).ShouldBe("-5");
    }

    [Fact]
    public void an_identifier_that_would_close_its_own_quoting_is_escaped()
    {
        // Nothing upstream of a column map is a sanitizing boundary — DbObjectName validates
        // nothing and the mapping API takes the name the caller wrote.
        new MemberMap("od]d", typeof(int)).UpdateExpression(SqlServer, "@p1")
            .ShouldBe("[od]]d] = @p1");

        new MemberMap("od\"d", typeof(int)).UpdateExpression(Sqlite, "@p1")
            .ShouldBe("\"od\"\"d\" = @p1");
    }

    [Fact]
    public void a_reserved_word_column_is_quoted_on_postgresql()
    {
        // PostgresqlDdlSyntax quotes only what has to be quoted, which is why the ordinary
        // expectations above are bare — and why this one is not.
        new MemberMap("order", typeof(int)).UpdateExpression(Postgresql, "p_order")
            .ShouldBe("\"order\" = p_order");
    }
}
