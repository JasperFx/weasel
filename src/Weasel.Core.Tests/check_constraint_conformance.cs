using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     A check constraint is either emitted or refused — never accepted and then left out of the
///     DDL (weasel#488).
/// </summary>
/// <remarks>
///     <para>
///         <c>TableBase.CheckConstraints</c> is on the shared base, so every provider's
///         <c>Table</c> accepted one. Only PostgreSQL and SQL Server ever wrote it into
///         <c>CREATE TABLE</c>. On the other three the constraint sat in the model, never reached
///         the database, and was never compared during delta detection either — so a caller got a
///         table without the constraint they asked for and nothing said so.
///     </para>
///     <para>
///         This is the rule weasel#449 settled for the index predicates that did the same thing,
///         applied to the one place it had been missed. It was missed because the property lives on
///         <c>TableBase</c> rather than on each provider's own type, so there was no per-provider
///         surface to audit — which is the argument for testing it here, across all five at once.
///     </para>
/// </remarks>
public class check_constraint_conformance
{
    public static TheoryData<string, Func<ITable>, bool> Providers =>
        new()
        {
            { "SqlServer", () => new SqlServer.Tables.Table("dbo.orders"), true },
            { "Postgresql", () => new Postgresql.Tables.Table("public.orders"), true },
            { "MySql", () => new MySql.Tables.Table("weasel_testing.orders"), false },
            { "Oracle", () => new Oracle.Tables.Table("WEASEL.ORDERS"), false },
            { "Sqlite", () => new Sqlite.Tables.Table("orders"), false }
        };

    private static ITable OrdersTable(Func<ITable> factory)
    {
        var table = factory();
        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("quantity", typeof(int));
        return table;
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void a_check_constraint_is_kept_or_refused_but_never_ignored(
        string provider, Func<ITable> factory, bool emits)
    {
        var table = OrdersTable(factory);

        if (emits)
        {
            table.AddCheckConstraint("ck_orders_qty", "quantity > 0");
            table.CheckConstraints.ShouldHaveSingleItem();
            return;
        }

        Should.Throw<NotSupportedException>(() => table.AddCheckConstraint("ck_orders_qty", "quantity > 0"));
        table.CheckConstraints.ShouldBeEmpty();
    }

    /// <summary>
    ///     Adding to the collection directly is the more common spelling, and used to be the way
    ///     round the refusal — so the collection refuses too, not only
    ///     <see cref="ITable.AddCheckConstraint" />.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void adding_to_the_collection_directly_is_refused_the_same_way(
        string provider, Func<ITable> factory, bool emits)
    {
        if (emits) return;

        var table = OrdersTable(factory);
        var collection = CheckConstraintsOf(table);

        Should.Throw<NotSupportedException>(
            () => collection.Add(new TableCheckConstraint("ck_orders_qty", "quantity > 0")));
    }

    /// <summary>
    ///     The message has to say which provider, that the engine supports it, and where to look —
    ///     otherwise it reads as "you cannot do this" rather than "Weasel does not do this yet".
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void the_refusal_explains_itself(string provider, Func<ITable> factory, bool emits)
    {
        if (emits) return;

        var table = OrdersTable(factory);

        var ex = Should.Throw<NotSupportedException>(
            () => table.AddCheckConstraint("ck_orders_qty", "quantity > 0"));

        ex.Message.ShouldContain("ck_orders_qty");
        ex.Message.ShouldContain("weasel#488");
    }

    /// <summary>
    ///     A provider that does emit them keeps working exactly as before — the constraint reaches
    ///     the generated DDL.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_provider_that_emits_them_still_writes_it_into_the_ddl(
        string provider, Func<ITable> factory, bool emits)
    {
        if (!emits) return;

        var table = OrdersTable(factory);
        table.AddCheckConstraint("ck_orders_qty", "quantity > 0");

        var writer = new StringWriter();
        table.WriteCreateStatement(MigratorFor(provider), writer);

        writer.ToString().ShouldContain("ck_orders_qty");
    }

    private static IList<TableCheckConstraint> CheckConstraintsOf(ITable table)
        => table switch
        {
            SqlServer.Tables.Table t => t.CheckConstraints,
            Postgresql.Tables.Table t => t.CheckConstraints,
            MySql.Tables.Table t => t.CheckConstraints,
            Oracle.Tables.Table t => t.CheckConstraints,
            Sqlite.Tables.Table t => t.CheckConstraints,
            _ => throw new ArgumentOutOfRangeException(nameof(table))
        };

    private static Migrator MigratorFor(string provider)
        => provider switch
        {
            "SqlServer" => new SqlServer.SqlServerMigrator { Formatting = SqlFormatting.Concise },
            "Postgresql" => new Postgresql.PostgresqlMigrator { Formatting = SqlFormatting.Concise },
            _ => throw new ArgumentOutOfRangeException(nameof(provider), provider, null)
        };
}
