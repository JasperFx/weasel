using Shouldly;
using Weasel.Postgresql;
using Weasel.Sqlite;
using Weasel.SqlServer;
using Weasel.Storage.Flattened;
using Xunit;
using PostgresqlTable = Weasel.Postgresql.Tables.Table;
using SqlServerTable = Weasel.SqlServer.Tables.Table;
using SqliteTable = Weasel.Sqlite.Tables.Table;

namespace Weasel.Core.Tests.Flattened;

/// <summary>
///     The whole statement, per upsert form — and the parameter numbering it has to agree with.
/// </summary>
/// <remarks>
///     The column list, the value list and the parameter order are one contract: the caller binds
///     parameter 0 to the key and 1..N to the maps that read from the event, in declaration order. A
///     map that takes no parameter must not consume a number, which is the single easiest thing to
///     get wrong here and shows up as every column after it holding the wrong value.
/// </remarks>
public class flat_table_statement_composition
{
    private record Deposit(decimal Amount);

    private static IReadOnlyList<IColumnMap> Columns() =>
    [
        new MemberMap("amount", typeof(decimal)),
        new IncrementMap("deposit_count"),
        new IncrementMemberMap("total", typeof(decimal)),
        new SetStringValueMap("status", "open")
    ];

    private static SqlServerTable SqlServer()
    {
        var table = new SqlServerTable(new SqlServerObjectName("dbo", "account"));
        ((ITable)table).AddPrimaryKeyColumn("id", typeof(Guid));

        return table;
    }

    private static SqliteTable Sqlite()
    {
        var table = new SqliteTable(new SqliteObjectName("main", "account"));
        ((ITable)table).AddPrimaryKeyColumn("id", typeof(Guid));

        return table;
    }

    private static PostgresqlTable Postgresql()
    {
        var table = new PostgresqlTable(new PostgresqlObjectName("public", "account"));
        ((ITable)table).AddPrimaryKeyColumn("id", typeof(Guid));

        return table;
    }

    private static void Resolve(ITable table, IReadOnlyList<IColumnMap> columns)
    {
        foreach (var column in columns)
        {
            FlatTableStatementBuilder.ResolveColumn(table, column);
        }
    }

    [Fact]
    public void merge_is_composed_the_way_polecat_composes_it()
    {
        var table = SqlServer();
        var columns = Columns();
        Resolve(table, columns);

        var upsert = FlatTableStatementBuilder.Upsert(ReferenceFlatTableDialects.SqlServer, table,
            typeof(Deposit), columns);

        upsert.SchemaObjects.ShouldBeEmpty();

        upsert.Sql.ShouldBe("""
                            MERGE [dbo].[account] WITH (UPDLOCK, HOLDLOCK) AS target
                            USING (SELECT @p0 AS [id]) AS source ON target.[id] = source.[id]
                            WHEN MATCHED THEN UPDATE SET [amount] = @p1, [deposit_count] = target.[deposit_count] + 1, [total] = target.[total] + @p2, [status] = 'open'
                            WHEN NOT MATCHED THEN INSERT ([id], [amount], [deposit_count], [total], [status]) VALUES (@p0, @p1, 1, @p2, 'open');
                            """);
    }

    [Fact]
    public void on_conflict_is_composed_the_way_fisher_composes_it()
    {
        var table = Sqlite();
        var columns = Columns();
        Resolve(table, columns);

        var upsert = FlatTableStatementBuilder.Upsert(ReferenceFlatTableDialects.Sqlite, table,
            typeof(Deposit), columns);

        upsert.SchemaObjects.ShouldBeEmpty();

        // Unquoted because Weasel's SQLite identifier rules quote only what has to be quoted — the
        // same rule Fisher's own flat table goes through.
        upsert.Sql.ShouldBe("""
                            insert into account (id, amount, deposit_count, total, status)
                            values (@p0, @p1, 1, @p2, 'open')
                            on conflict (id) do update set amount = @p1, deposit_count = deposit_count + 1, total = total + @p2, status = 'open';
                            """);
    }

    [Fact]
    public void the_function_form_returns_the_function_alongside_the_call()
    {
        var table = Postgresql();
        var columns = Columns();
        Resolve(table, columns);

        var upsert = FlatTableStatementBuilder.Upsert(ReferenceFlatTableDialects.Postgresql, table,
            typeof(Deposit), columns);

        // The shape the seam exists to accommodate: the runtime statement is a call, and the object
        // it calls comes back with it to be created by the caller's migration.
        upsert.Sql.ShouldBe("select public.mt_upsert_account_deposit(?, ?, ?)");

        var function = upsert.SchemaObjects.ShouldHaveSingleItem();
        function.Identifier.Name.ShouldBe("mt_upsert_account_deposit");

        var body = WriteCreateStatement(function);

        body.ShouldContain("ON CONFLICT ON CONSTRAINT pkey_account_id");
        body.ShouldContain(
            "DO UPDATE SET amount = p_amount, deposit_count = account.deposit_count + 1, "
            + "total = account.total + p_total, status = 'open';");
        body.ShouldContain(
            "INSERT INTO public.account (id, amount, deposit_count, total, status) "
            + "VALUES (p_id, p_amount, 1, p_total, 'open')");
    }

    [Fact]
    public void only_the_maps_that_read_the_event_consume_a_parameter()
    {
        var table = Sqlite();

        // Two literal maps sandwiched between the member maps: the numbering must skip them, or the
        // caller's setters bind to the wrong slots.
        IReadOnlyList<IColumnMap> columns =
        [
            new SetIntValueMap("a", 0),
            new MemberMap("b", typeof(int)),
            new IncrementMap("c"),
            new MemberMap("d", typeof(int))
        ];

        Resolve(table, columns);

        var sql = FlatTableStatementBuilder
            .Upsert(ReferenceFlatTableDialects.Sqlite, table, typeof(Deposit), columns).Sql;

        sql.ShouldContain("values (@p0, 0, @p1, 1, @p2)");
        sql.ShouldContain("do update set a = 0, b = @p1, c = c + 1, d = @p2;");
    }

    [Fact]
    public void delete_is_keyed_on_parameter_zero()
    {
        FlatTableStatementBuilder.Delete(ReferenceFlatTableDialects.Sqlite, Sqlite())
            .ShouldBe("delete from account where id = @p0;");

        FlatTableStatementBuilder.Delete(ReferenceFlatTableDialects.SqlServer, SqlServer())
            .ShouldBe("DELETE FROM [dbo].[account] WHERE [id] = @p0;");
    }

    [Fact]
    public void a_table_with_no_primary_key_is_refused_by_name()
    {
        var table = new SqliteTable(new SqliteObjectName("main", "account"));
        ((ITable)table).AddColumn("amount", typeof(decimal));

        var ex = Should.Throw<InvalidOperationException>(() =>
            FlatTableStatementBuilder.Upsert(ReferenceFlatTableDialects.Sqlite, table, typeof(Deposit),
                [new MemberMap("amount", typeof(decimal))]));

        ex.Message.ShouldContain("account");
        ex.Message.ShouldContain("AsPrimaryKey");
    }

    [Fact]
    public void mapping_no_columns_at_all_is_refused()
    {
        // An empty update branch is a syntax error, not a no-op. Name it here, where the projection
        // is still identifiable, rather than at the first event.
        Should.Throw<InvalidOperationException>(() =>
                FlatTableStatementBuilder.Upsert(ReferenceFlatTableDialects.Sqlite, Sqlite(),
                    typeof(Deposit), []))
            .Message.ShouldContain("at least one column");
    }

    [Fact]
    public void a_column_named_twice_in_different_casings_is_one_column()
    {
        // Identifiers are case-insensitive where values are not, so two Project<T> handlers naming
        // "Amount" and "amount" mean one column — and adding both emits DDL the database rejects as
        // a duplicate (polecat#45).
        var table = Sqlite();

        FlatTableStatementBuilder.ResolveColumn(table, new MemberMap("Amount", typeof(decimal)));
        FlatTableStatementBuilder.ResolveColumn(table, new MemberMap("amount", typeof(decimal)));

        table.Columns.Count(x => string.Equals(x.Name, "amount", StringComparison.OrdinalIgnoreCase))
            .ShouldBe(1);
    }

    [Fact]
    public void an_explicit_column_type_rule_wins_over_the_providers_own()
    {
        // The hook a store needs where it has an opinion the provider's type map does not carry —
        // a strong-typed id stored as its inner primitive, for instance.
        var table = Sqlite();

        FlatTableStatementBuilder.ResolveColumn(table, new MemberMap("amount", typeof(decimal)),
            _ => "NUMERIC(19,4)");

        table.ColumnFor("amount")!.Type.ShouldBe("NUMERIC(19,4)");
    }

    [Fact]
    public void the_insert_branch_is_pinned_where_the_stores_disagreed()
    {
        // Pinned as a decision rather than left to be discovered at adoption, and the two halves
        // were settled separately.
        //
        // Increment(column) inserts 1 — marten#5341's ruling, which Marten then adopted in #5342, so
        // all four stores agree.
        //
        // Decrement(member) inserts the *negated* parameter — jasperfx#773's ruling. That was
        // Marten's shape alone; Polecat, Fisher and the first cut of these maps inserted the
        // parameter as given, which made a decrement onto a missing row leave the column positive.
        // Adopting these maps is therefore a behaviour change for Polecat and Fisher, deliberately.
        var table = Sqlite();

        IReadOnlyList<IColumnMap> columns =
        [
            new IncrementMap("hits"),
            new DecrementMemberMap("balance", typeof(decimal))
        ];

        Resolve(table, columns);

        FlatTableStatementBuilder.Upsert(ReferenceFlatTableDialects.Sqlite, table, typeof(Deposit), columns)
            .Sql.ShouldContain("values (@p0, 1, -@p1)");
    }

    private static string WriteCreateStatement(ISchemaObject schemaObject)
    {
        var writer = new StringWriter();
        schemaObject.WriteCreateStatement(new PostgresqlMigrator(), writer);

        return writer.ToString();
    }
}
