using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests;

/// <summary>
///     weasel#634. Coverage for the <see cref="SchemaUtils.DropSchema(string,string)" /> retry loop, exercised through
///     the internal overload that takes the single attempt as a delegate. The attempt only reports failure
///     for <c>57P01 admin_shutdown</c>, so the exhausted-retries path needed a server that stays down across
///     three tries to reach — which is why it went unnoticed that the loop returned on the last failed
///     attempt as though the drop had worked, making the trailing throw unreachable.
/// </summary>
public class SchemaUtilsDropSchemaRetryTests
{
    [Fact]
    public async Task succeeds_on_the_first_attempt_without_retrying()
    {
        var attempts = 0;

        await SchemaUtils.DropSchema("Host=nowhere", "foo", (_, _) =>
        {
            attempts++;
            return Task.FromResult(true);
        });

        attempts.ShouldBe(1);
    }

    [Fact]
    public async Task retries_until_an_attempt_succeeds()
    {
        var attempts = 0;

        await SchemaUtils.DropSchema("Host=nowhere", "foo", (_, _) =>
        {
            attempts++;
            return Task.FromResult(attempts == 3);
        });

        attempts.ShouldBe(3);
    }

    [Fact]
    public async Task throws_when_every_attempt_fails()
    {
        var attempts = 0;

        var ex = await Should.ThrowAsync<InvalidOperationException>(() =>
            SchemaUtils.DropSchema("Host=nowhere", "foo", (_, _) =>
            {
                attempts++;
                return Task.FromResult(false);
            }));

        // The whole point: the last failed attempt has to reach the throw instead of returning quietly.
        ex.Message.ShouldBe("Unable to drop schema: foo");
        attempts.ShouldBe(3);
    }

    [Fact]
    public async Task passes_the_connection_string_and_schema_name_to_each_attempt()
    {
        var calls = new List<(string ConnectionString, string SchemaName)>();

        await Should.ThrowAsync<InvalidOperationException>(() =>
            SchemaUtils.DropSchema("Host=localhost;Database=weasel_testing", "some_schema", (cs, schema) =>
            {
                calls.Add((cs, schema));
                return Task.FromResult(false);
            }));

        calls.ShouldAllBe(x => x.ConnectionString == "Host=localhost;Database=weasel_testing");
        calls.ShouldAllBe(x => x.SchemaName == "some_schema");
    }

    [Fact]
    public async Task does_not_swallow_an_exception_thrown_by_an_attempt()
    {
        // dropSchema() rethrows anything that is not admin_shutdown; the retry must not convert that
        // into its own generic failure or bury it behind more attempts.
        var attempts = 0;

        var ex = await Should.ThrowAsync<DivideByZeroException>(() =>
            SchemaUtils.DropSchema("Host=nowhere", "foo", (_, _) =>
            {
                attempts++;
                throw new DivideByZeroException("boom");
            }));

        ex.Message.ShouldBe("boom");
        attempts.ShouldBe(1);
    }
}
