using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     The name a caller gives a column is the name of the column that gets created, and everything
///     that references that column agrees with it. Five providers used to do four different things
///     here, and three of them rewrote a space into an underscore — but only in
///     <c>TableColumn</c>, not in the index, foreign key or primary key column lists, so an index
///     over <c>Order Date</c> named a column that did not exist (weasel#458).
/// </summary>
/// <remarks>
///     <para>
///         Rewriting was never anyone's decision — it predates the identifier work, and weasel#448
///         settled that a name is either honoured (quoted where it needs quoting, per weasel#447)
///         or rejected. Silently changing it is the third option, and nobody chose it.
///     </para>
///     <para>
///         Case folding is a separate question and stays: PostgreSQL, Oracle and SQLite still fold
///         to lowercase unless <see cref="ITable.PreserveIdentifierCase" /> is set. What changed is
///         that the flag now controls that and nothing else — it used to switch off the space
///         rewrite as a side effect, so two unrelated decisions travelled on one bool.
///     </para>
/// </remarks>
public class column_name_conformance
{
    public static TheoryData<string, Func<ITable>> Providers =>
        new()
        {
            { "SqlServer", () => new SqlServer.Tables.Table("dbo.orders") },
            { "MySql", () => new MySql.Tables.Table("weasel_testing.orders") },
            { "Sqlite", () => new Sqlite.Tables.Table("orders") },
            { "Postgresql", () => new Postgresql.Tables.Table("public.orders") },
            { "Oracle", () => new Oracle.Tables.Table("WEASEL.ORDERS") }
        };

    [Theory]
    [MemberData(nameof(Providers))]
    public void a_space_in_a_column_name_is_not_rewritten(string provider, Func<ITable> factory)
    {
        var table = factory();
        table.AddColumn("Order Date", typeof(DateTime));

        table.Columns.Single().Name
            .ShouldBe("order date", StringCompareShould.IgnoreCase);
    }

    /// <summary>
    ///     The bug in the issue title. The column list and the column have to name the same thing.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void an_index_over_a_spaced_column_references_the_column_that_exists(
        string provider, Func<ITable> factory)
    {
        var table = factory();
        table.AddColumn("Order Date", typeof(DateTime));
        table.AddIndex("ix_orders_date", ["Order Date"]);

        var created = table.Columns.Single().Name;
        var referenced = table.Indexes.Single().Columns!.Single();

        referenced.ShouldBe(created, StringCompareShould.IgnoreCase);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void a_foreign_key_over_a_spaced_column_references_the_column_that_exists(
        string provider, Func<ITable> factory)
    {
        var table = factory();
        table.AddColumn("Customer Id", typeof(int));
        table.AddForeignKey("fk_orders_customer", table.Identifier, ["Customer Id"], ["id"]);

        var created = table.Columns.Single().Name;
        var referenced = table.ForeignKeys.Single().ColumnNames.Single();

        referenced.ShouldBe(created, StringCompareShould.IgnoreCase);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void a_primary_key_over_a_spaced_column_references_the_column_that_exists(
        string provider, Func<ITable> factory)
    {
        var table = factory();
        table.AddPrimaryKeyColumn("Order Id", typeof(int));

        var created = table.Columns.Single().Name;

        table.PrimaryKeyColumns.Single().ShouldBe(created, StringCompareShould.IgnoreCase);
    }

    /// <summary>
    ///     The DDL is where the mismatch actually bit: the model could be self-consistent and the
    ///     generated statement still name something else, because the index writers apply their own
    ///     quoting heuristics. Asserting on the emitted text catches both halves at once.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void the_generated_ddl_names_the_same_column_everywhere(string provider, Func<ITable> factory)
    {
        var table = factory();
        table.AddPrimaryKeyColumn("Order Id", typeof(int));
        table.AddColumn("Order Date", typeof(DateTime));
        table.AddIndex("ix_orders_date", ["Order Date"]);

        var writer = new StringWriter();
        table.WriteCreateStatement(MigratorFor(provider), writer);
        var ddl = writer.ToString();

        // Whatever the provider delimits with, the name inside it is the caller's -- never
        // Order_Date, and never bare across the space where the parser would split it in two.
        ddl.ShouldNotContain("Order_Date", Case.Insensitive,
            $"{provider} still rewrites a space into an underscore");
        ddl.ShouldContain("Order Date", Case.Insensitive,
            $"{provider} did not emit the column the caller asked for");
    }

    private static Migrator MigratorFor(string provider)
        => provider switch
        {
            "SqlServer" => new SqlServer.SqlServerMigrator { Formatting = SqlFormatting.Concise },
            "MySql" => new MySql.MySqlMigrator { Formatting = SqlFormatting.Concise },
            "Sqlite" => new Sqlite.SqliteMigrator { Formatting = SqlFormatting.Concise },
            "Postgresql" => new Postgresql.PostgresqlMigrator { Formatting = SqlFormatting.Concise },
            "Oracle" => new Oracle.OracleMigrator { Formatting = SqlFormatting.Concise },
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null)
        };
}
