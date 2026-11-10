using Npgsql;
using Shouldly;
using Weasel.Core.Tests.StorageHelpers;
using Weasel.Storage;
using Xunit;

namespace Weasel.Postgresql.Tests.Storage;

/// <summary>
///     weasel#744: <see cref="PostgresqlUnionSqlDialect"/> run against a real server over the shared
///     <see cref="UnionSqlScenarios"/>. The rows are a <c>values</c> list cast to <c>jsonb</c>, so
///     nothing is created on the server.
/// </summary>
public class union_sql_dialect_against_postgresql
{
    private static readonly PostgresqlUnionSqlDialect Dialect = PostgresqlUnionSqlDialect.Instance;

    private static string Source()
    {
        var rows = UnionSqlScenarios.Rows.Select(r => $"({r.Id}, '{r.Json.Replace("'", "''")}'::jsonb)");
        return $"(values {string.Join(", ", rows)}) as d(id, data)";
    }

    [Theory]
    [MemberData(nameof(UnionSqlScenarios.TypeTestNames), MemberType = typeof(UnionSqlScenarios))]
    public async Task type_test_selects_exactly_the_case(string scenario)
    {
        var (path, test, expected) = UnionSqlScenarios.TypeTests[scenario];
        var predicate = Dialect.TypeTest("d.data", path, test);

        await using var connection = await openAsync();
        await using var command = new NpgsqlCommand($"select id from {Source()} where {predicate} order by id", connection);

        var ids = new List<int>();
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            ids.Add(reader.GetInt32(0));
        }

        ids.ShouldBe(expected);
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
        var payment = await scalarAsync(connection, $"select ({json})::text from {Source()} where id = 7");
        UnionSqlScenarios.NormalizeJson((string)payment!).ShouldBe("""{"Amount":20,"Reason":"rent"}""");
    }

    private static async Task<object?> scalarAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(TestContext.Current.CancellationToken);
    }

    private static async Task<NpgsqlConnection> openAsync()
    {
        var connection = new NpgsqlConnection(ConnectionSource.ConnectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return connection;
    }
}
