using Microsoft.Data.Sqlite;
using NSubstitute;
using Shouldly;
using Weasel.Core.SqlGeneration;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

/// <summary>
///     weasel#744: the rendered SQL of the three <see cref="IUnionSqlDialect"/> implementations, and
///     SQLite's run for real against the shared <see cref="UnionSqlScenarios"/>. PostgreSQL's and SQL
///     Server's live runs are in their own test projects, where the servers are.
/// </summary>
public class union_sql_dialect
{
    private static readonly UnionCaseTest Deposited = UnionCaseTest.ObjectWith(["Amount"], ["Reason"]);

    [Fact]
    public void postgresql_renders_jsonb_typeof_and_presence_without_the_question_mark_operator()
    {
        var sql = PostgresqlUnionSqlDialect.Instance.TypeTest("d.data", ["Payment"], Deposited);

        sql.ShouldBe(
            "(jsonb_typeof(d.data -> 'Payment') = 'object' and (d.data -> 'Payment' -> 'Amount') is not null and (d.data -> 'Payment' -> 'Reason') is null)");
        sql.ShouldNotContain("?");
    }

    [Fact]
    public void postgresql_case_access()
    {
        var dialect = PostgresqlUnionSqlDialect.Instance;
        dialect.CaseAccessAsText("d.data", ["Payment"], ["Reason"]).ShouldBe("d.data -> 'Payment' ->> 'Reason'");
        dialect.CaseAccessAsJson("d.data", ["Payment"], []).ShouldBe("d.data -> 'Payment'");
    }

    [Fact]
    public void sqlserver_renders_openjson_type_and_keys()
    {
        var sql = SqlServerUnionSqlDialect.Instance.TypeTest("d.data", ["Outer", "Payment"], Deposited);

        sql.ShouldBe(
            "(exists (select 1 from OPENJSON(d.data, N'$.\"Outer\"') where [key] = N'Payment' and [type] = 5) and exists (select 1 from OPENJSON(d.data, N'$.\"Outer\".\"Payment\"') where [key] = N'Amount') and not exists (select 1 from OPENJSON(d.data, N'$.\"Outer\".\"Payment\"') where [key] = N'Reason'))");
        sql.ShouldNotContain("JSON_PATH_EXISTS"); // SQL Server 2022+ only; the 2019 engine refuses it
    }

    [Fact]
    public void sqlserver_case_access()
    {
        var dialect = SqlServerUnionSqlDialect.Instance;
        dialect.CaseAccessAsText("d.data", ["Payment"], ["Reason"]).ShouldBe("JSON_VALUE(d.data, N'$.\"Payment\".\"Reason\"')");
        dialect.CaseAccessAsJson("d.data", ["Payment"], []).ShouldBe("JSON_QUERY(d.data, N'$.\"Payment\"')");
    }

    [Fact]
    public void sqlite_renders_json_type_with_the_split_kinds()
    {
        var dialect = SqliteUnionSqlDialect.Instance;

        dialect.TypeTest("d.data", ["Payment"], UnionCaseTest.OfKind(UnionCaseJsonKind.Number))
            .ShouldBe("json_type(d.data, '$.\"Payment\"') in ('integer', 'real')");
        dialect.TypeTest("d.data", ["Payment"], UnionCaseTest.OfKind(UnionCaseJsonKind.Boolean))
            .ShouldBe("json_type(d.data, '$.\"Payment\"') in ('true', 'false')");
        dialect.TypeTest("d.data", ["Payment"], UnionCaseTest.ObjectWithDiscriminator("$kind", "card"))
            .ShouldBe("(json_type(d.data, '$.\"Payment\"') = 'object' and json_extract(d.data, '$.\"Payment\".\"$kind\"') = 'card')");
    }

    [Fact]
    public void sqlite_case_access()
    {
        var dialect = SqliteUnionSqlDialect.Instance;
        dialect.CaseAccessAsText("d.data", ["Payment"], ["Reason"]).ShouldBe("json_extract(d.data, '$.\"Payment\".\"Reason\"')");
        dialect.CaseAccessAsJson("d.data", ["Payment"], []).ShouldBe("(d.data -> '$.\"Payment\"')");
    }

    [Fact]
    public void the_fragment_appends_the_type_test()
    {
        IUnionSqlDialect dialect = PostgresqlUnionSqlDialect.Instance;
        var fragment = dialect.TypeTestFragment("d.data", ["Payment"], UnionCaseTest.OfKind(UnionCaseJsonKind.Null));

        var builder = Substitute.For<ICommandBuilder>();
        fragment.Apply(builder);
        builder.Received(1).Append("jsonb_typeof(d.data -> 'Payment') = 'null'");
    }

    public static TheoryData<string> Dialects() => new() { "postgresql", "sqlserver", "sqlite" };

    private static IUnionSqlDialect DialectNamed(string name) => name switch
    {
        "postgresql" => PostgresqlUnionSqlDialect.Instance,
        "sqlserver" => SqlServerUnionSqlDialect.Instance,
        _ => SqliteUnionSqlDialect.Instance
    };

    [Theory]
    [MemberData(nameof(Dialects))]
    public void a_primitive_case_cannot_be_tested_by_its_properties(string name)
    {
        var test = new UnionCaseTest(UnionCaseJsonKind.String) { RequiredProperties = ["Amount"] };
        Should.Throw<ArgumentException>(() => DialectNamed(name).TypeTest("d.data", ["Payment"], test));
    }

    [Theory]
    [MemberData(nameof(Dialects))]
    public void a_member_path_is_required(string name)
    {
        Should.Throw<ArgumentException>(() =>
            DialectNamed(name).TypeTest("d.data", [], UnionCaseTest.OfKind(UnionCaseJsonKind.Object)));
        Should.Throw<ArgumentException>(() =>
            DialectNamed(name).TypeTest("d.data", [""], UnionCaseTest.OfKind(UnionCaseJsonKind.Object)));
    }

    [Theory]
    [MemberData(nameof(UnionSqlScenarios.TypeTestNames), MemberType = typeof(UnionSqlScenarios))]
    public async Task sqlite_type_test_selects_exactly_the_case(string scenario)
    {
        var (path, test, expected) = UnionSqlScenarios.TypeTests[scenario];
        var predicate = SqliteUnionSqlDialect.Instance.TypeTest("d.data", path, test);

        var ids = new List<int>();
        await using var connection = await openWithRowsAsync();
        await using var command = connection.CreateCommand();
        command.CommandText = $"select id from d where {predicate} order by id";
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            ids.Add(reader.GetInt32(0));
        }

        ids.ShouldBe(expected);
    }

    [Fact]
    public async Task sqlite_case_access_reads_the_case_member()
    {
        await using var connection = await openWithRowsAsync();

        foreach (var (id, member, expected) in UnionSqlScenarios.TextAccess)
        {
            var access = SqliteUnionSqlDialect.Instance.CaseAccessAsText("d.data", UnionSqlScenarios.PaymentPath, [member]);
            var value = await scalarAsync(connection, $"select {access} from d where id = {id}");
            (value is DBNull or null ? null : Convert.ToString(value)).ShouldBe(expected, $"row {id}, {member}");
        }

        var json = SqliteUnionSqlDialect.Instance.CaseAccessAsJson("d.data", UnionSqlScenarios.PaymentPath, []);
        var payment = (string)(await scalarAsync(connection, $"select {json} from d where id = 7"))!;
        UnionSqlScenarios.NormalizeJson(payment).ShouldBe("""{"Amount":20,"Reason":"rent"}""");

        var scalar = (string)(await scalarAsync(connection, $"select {json} from d where id = 2"))!;
        scalar.ShouldBe("\"5\"");
    }

    [Fact]
    public void sqlite_refuses_a_name_its_path_syntax_cannot_address()
    {
        // SQLite's quoted path keys have no escape for an embedded double quote.
        Should.Throw<ArgumentException>(() =>
            SqliteUnionSqlDialect.Instance.TypeTest("d.data", ["a\"b"], UnionCaseTest.OfKind(UnionCaseJsonKind.Object)));
    }

    private static async Task<object?> scalarAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        return await command.ExecuteScalarAsync();
    }

    private static async Task<SqliteConnection> openWithRowsAsync()
    {
        var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await using var create = connection.CreateCommand();
        create.CommandText = "create table d (id integer primary key, data text not null)";
        await create.ExecuteNonQueryAsync();

        foreach (var (id, json) in UnionSqlScenarios.Rows)
        {
            await using var insert = connection.CreateCommand();
            insert.CommandText = "insert into d (id, data) values ($id, $data)";
            insert.Parameters.AddWithValue("$id", id);
            insert.Parameters.AddWithValue("$data", json);
            await insert.ExecuteNonQueryAsync();
        }

        return connection;
    }
}
