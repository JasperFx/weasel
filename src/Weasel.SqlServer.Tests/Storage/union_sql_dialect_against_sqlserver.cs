using Microsoft.Data.SqlClient;
using Shouldly;
using Weasel.Core.Tests.StorageHelpers;
using Weasel.Storage;
using Xunit;

namespace Weasel.SqlServer.Tests.Storage;

/// <summary>
///     weasel#744: <see cref="SqlServerUnionSqlDialect"/> run against a real server over the shared
///     <see cref="UnionSqlScenarios"/>. The rows are a <c>values</c> list of <c>nvarchar(max)</c>, so
///     nothing is created on the server. Runs on the 2019 engine, which has no
///     <c>JSON_PATH_EXISTS</c>.
/// </summary>
public class union_sql_dialect_against_sqlserver
{
    private static readonly SqlServerUnionSqlDialect Dialect = SqlServerUnionSqlDialect.Instance;

    private static string Source()
    {
        var rows = UnionSqlScenarios.Rows.Select(r =>
            $"({r.Id}, cast(N'{r.Json.Replace("'", "''")}' as nvarchar(max)))");
        return $"(values {string.Join(", ", rows)}) as d(id, data)";
    }

    [Theory]
    [MemberData(nameof(UnionSqlScenarios.TypeTestNames), MemberType = typeof(UnionSqlScenarios))]
    public async Task type_test_selects_exactly_the_case(string scenario)
    {
        var (path, test, expected) = UnionSqlScenarios.TypeTests[scenario];
        var predicate = Dialect.TypeTest("d.data", path, test);

        await using var connection = await openAsync();
        await using var command = new SqlCommand($"select id from {Source()} where {predicate} order by id", connection);

        var ids = new List<int>();
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            ids.Add(reader.GetInt32(0));
        }

        ids.ShouldBe(expected);
    }

    [Fact]
    public async Task a_key_with_a_double_quote_is_escaped_in_the_path()
    {
        await using var connection = await openAsync();
        var source = """(values (1, cast(N'{"a\"b": {"Amount": 1}}' as nvarchar(max)))) as d(id, data)""";

        var predicate = Dialect.TypeTest("d.data", ["a\"b"], UnionCaseTest.ObjectWith(["Amount"]));
        var count = await scalarAsync(connection, $"select count(*) from {source} where {predicate}");
        count.ShouldBe(1);

        var access = Dialect.CaseAccessAsText("d.data", ["a\"b"], ["Amount"]);
        (await scalarAsync(connection, $"select {access} from {source}")).ShouldBe("1");
    }

    [Fact]
    public async Task case_access_reads_the_case_member()
    {
        await using var connection = await openAsync();

        foreach (var (id, member, expected) in UnionSqlScenarios.TextAccess)
        {
            var access = Dialect.CaseAccessAsText("d.data", UnionSqlScenarios.PaymentPath, [member]);
            var value = await scalarAsync(connection, $"select {access} from {Source()} where id = {id}");
            (value is DBNull or null ? null : Convert.ToString(value)).ShouldBe(expected, $"row {id}, {member}");
        }

        var json = Dialect.CaseAccessAsJson("d.data", UnionSqlScenarios.PaymentPath, []);
        var payment = await scalarAsync(connection, $"select {json} from {Source()} where id = 7");
        UnionSqlScenarios.NormalizeJson((string)payment!).ShouldBe("""{"Amount":20,"Reason":"rent"}""");
    }

    private static async Task<object?> scalarAsync(SqlConnection connection, string sql)
    {
        await using var command = new SqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<SqlConnection> openAsync()
    {
        var connection = new SqlConnection(ConnectionSource.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }
}
