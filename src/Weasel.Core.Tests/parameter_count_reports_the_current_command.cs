using System.Data.Common;
using FirebirdSql.Data.FirebirdClient;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using Shouldly;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     <see cref="ICommandBuilder.ParameterCount" /> has to answer for the command being built right
///     now, on every builder shape, because that is the only count comparable against
///     <see cref="Migrator.MaxParametersPerCommand" />.
/// </summary>
/// <remarks>
///     <para>
///     weasel#675. The budget belongs to the command, not to the fragment that is rendering, so two
///     independent fragments each well under any sane per-fragment threshold can still take a command
///     over the limit between them. The second fragment can only know that if it can read what the
///     first one spent.
///     </para>
///     <para>
///     Weasel has three builder shapes and they do not agree by construction, which is the whole
///     reason for a cross-provider test rather than one per provider. <see cref="CommandBuilderBase{TCommand,TParameter,TDbType}" />
///     fills one command and reads straight off it. The PostgreSQL and SQL Server <c>BatchBuilder</c>s
///     deal statements into separate batch commands. And the Oracle and Firebird
///     <c>DbCommandBuilder</c>s bind every parameter onto <b>one</b> underlying command and only split
///     it at compile time — so the base class's answer there is the batch-wide total, not the current
///     statement's, and both override it. That case is the one this test exists for; it is invisible to
///     a test written against any single provider.
///     </para>
/// </remarks>
public class parameter_count_reports_the_current_command
{
    /// <summary>
    ///     Every builder that reaches the dialect-neutral interface, in the shape a consumer gets it.
    /// </summary>
    public static TheoryData<string, ICommandBuilder> Builders() => new()
    {
        { "Postgresql.CommandBuilder", new Postgresql.CommandBuilder(new NpgsqlCommand()) },
        { "Postgresql.BatchBuilder", new Postgresql.BatchBuilder() },
        { "SqlServer.CommandBuilder", new SqlServer.CommandBuilder(new SqlCommand()) },
        { "SqlServer.BatchBuilder", new SqlServer.BatchBuilder() },
        { "MySql.CommandBuilder", new MySql.CommandBuilder(new MySqlCommand()) },
        { "Sqlite.CommandBuilder", new Sqlite.CommandBuilder(new SqliteCommand()) },
        { "Oracle.CommandBuilder", new Oracle.CommandBuilder(new OracleCommand()) },
        { "Firebird.CommandBuilder", new Firebird.CommandBuilder(new FbCommand()) }
    };

    [Theory]
    [MemberData(nameof(Builders))]
    public void a_fresh_builder_carries_nothing(string name, ICommandBuilder builder)
    {
        builder.ParameterCount.ShouldBe(0, name);
    }

    [Theory]
    [MemberData(nameof(Builders))]
    public void counts_parameters_appended_one_at_a_time(string name, ICommandBuilder builder)
    {
        builder.Append("select * from foo where a = ");
        builder.AppendParameter(1);
        builder.ParameterCount.ShouldBe(1, name);

        builder.Append(" and b = ");
        builder.AppendParameter(2);
        builder.ParameterCount.ShouldBe(2, name);
    }

    [Theory]
    [MemberData(nameof(Builders))]
    public void counts_parameters_appended_as_a_group(string name, ICommandBuilder builder)
    {
        builder.Append("select * from foo where a in (");
        builder.AppendParameters("one", "two", "three");
        builder.Append(')');

        builder.ParameterCount.ShouldBe(3, name);
    }

    /// <summary>
    ///     The two paths the issue singles out, because an index parsed back out of
    ///     <see cref="ICommandBuilder.LastParameterName" /> is simply wrong for them: neither goes
    ///     through <see cref="ParameterNames.ForPosition" />.
    /// </summary>
    [Theory]
    [MemberData(nameof(Builders))]
    public void counts_the_paths_that_do_not_name_by_position(string name, ICommandBuilder builder)
    {
        builder.Append("select * from foo where a = ? and b = ?");
        builder.AppendWithDbParameters("select * from foo where c = ? and d = ?")
            .Length.ShouldBe(2, name);

        builder.ParameterCount.ShouldBe(2, name);

        builder.AddParameters(new Dictionary<string, object?> { ["named"] = 5 });

        builder.ParameterCount.ShouldBe(3, name);
    }

    /// <summary>
    ///     The scenario from the issue: two fragments that know nothing about each other, where the
    ///     second has to be able to see the first's spend.
    /// </summary>
    [Theory]
    [MemberData(nameof(Builders))]
    public void a_later_fragment_sees_what_an_earlier_one_spent(string name, ICommandBuilder builder)
    {
        appendValues(builder, 1_500);
        var afterFirst = builder.ParameterCount;

        appendValues(builder, 600);

        afterFirst.ShouldBe(1_500, name);
        builder.ParameterCount.ShouldBe(2_100, name);

        // Which is what makes the decision possible at all: neither fragment is over on its own,
        // and together they are past SQL Server's budget.
        afterFirst.ShouldBeLessThan(new SqlServer.SqlServerMigrator().MaxParametersPerCommand);
        builder.ParameterCount.ShouldBeGreaterThan(new SqlServer.SqlServerMigrator().MaxParametersPerCommand);
    }

    private static void appendValues(ICommandBuilder builder, int count)
    {
        builder.Append(" and x in (");
        for (var i = 0; i < count; i++)
        {
            if (i > 0) builder.Append(',');
            builder.AppendParameter(i);
        }

        builder.Append(')');
    }

    /// <summary>
    ///     The neutral builders that really do spread work over several commands. A no-op
    ///     <see cref="ICommandBuilder.StartNewCommand" /> is covered by
    ///     <see cref="keeps_counting_across_a_no_op_boundary" /> instead — the count must not reset
    ///     there, because there is still only one command.
    /// </summary>
    public static TheoryData<string, ICommandBuilder> SplittingBuilders() => new()
    {
        { "Postgresql.BatchBuilder", new Postgresql.BatchBuilder() },
        { "SqlServer.BatchBuilder", new SqlServer.BatchBuilder() }
    };

    [Theory]
    [MemberData(nameof(SplittingBuilders))]
    public void resets_at_a_command_boundary_that_really_splits(string name, ICommandBuilder builder)
    {
        assertResetsAtBoundary(name, builder.Append, builder.Append, value => builder.AppendParameter(value),
            builder.StartNewCommand, () => builder.ParameterCount);
    }

    /// <summary>
    ///     Oracle's and Firebird's drivers execute one statement per command, so their
    ///     <c>DbCommandBuilder</c>s split a batch — but they bind every statement's parameters onto
    ///     <b>one</b> underlying command and only deal them out at compile time. So
    ///     <see cref="CommandBuilderBase{TCommand,TParameter,TDbType}.ParameterCount" /> would carry
    ///     the first statement's parameters into the second, over-reporting the budget to a caller
    ///     whose whole purpose is to stay under it. Both override it; this is what holds that.
    /// </summary>
    /// <remarks>
    ///     These two do not reach <see cref="ICommandBuilder" /> — <c>DbCommandBuilder</c> implements
    ///     only the generic <c>ICommandBuilder&lt;TCommand&gt;</c> — so they are exercised as their
    ///     concrete selves rather than through the neutral surface. They are still public types a
    ///     consumer builds against, and <c>ParameterCount</c> is still public on them.
    /// </remarks>
    [Theory]
    [InlineData("Oracle")]
    [InlineData("Firebird")]
    public void a_splitting_db_command_builder_counts_only_the_open_statement(string provider)
    {
        DbCommandBuilder builder = provider == "Oracle"
            ? new Weasel.Oracle.OracleDbCommandBuilder()
            : new Weasel.Firebird.FirebirdDbCommandBuilder();

        assertResetsAtBoundary(provider, builder.Append, builder.Append,
            value => builder.AppendParameter(value), builder.StartNewCommand, () => builder.ParameterCount);

        // And the underlying command really does still hold all three, which is why reading straight
        // off it was the wrong answer here
        builder.Compile().Parameters.Count.ShouldBe(3, provider);
    }

    private static void assertResetsAtBoundary(string name, Action<string> appendSql, Action<char> appendChar,
        Action<object> appendParameter, Action startNewCommand, Func<int> parameterCount)
    {
        appendSql("select * from foo where a = ");
        appendParameter(1);
        appendSql(" and b = ");
        appendParameter(2);

        parameterCount().ShouldBe(2, name);

        appendChar(';');
        startNewCommand();

        parameterCount().ShouldBe(0, name);

        appendSql("select * from bar where c = ");
        appendParameter(3);

        parameterCount().ShouldBe(1, name);
    }

    /// <summary>
    ///     The other side of it. On a provider that concatenates, <c>StartNewCommand</c> is a no-op and
    ///     a reset would be a lie: everything appended still lands on one command, against one budget.
    /// </summary>
    [Theory]
    [InlineData("Postgresql")]
    [InlineData("SqlServer")]
    [InlineData("MySql")]
    [InlineData("Sqlite")]
    public void keeps_counting_across_a_no_op_boundary(string provider)
    {
        ICommandBuilder builder = provider switch
        {
            "Postgresql" => new Postgresql.CommandBuilder(new NpgsqlCommand()),
            "SqlServer" => new SqlServer.CommandBuilder(new SqlCommand()),
            "MySql" => new MySql.CommandBuilder(new MySqlCommand()),
            _ => new Sqlite.CommandBuilder(new SqliteCommand())
        };

        builder.Append("select 1 where a = ");
        builder.AppendParameter(1);
        builder.Append(';');

        builder.StartNewCommand();

        builder.Append("select 2 where b = ");
        builder.AppendParameter(2);

        builder.ParameterCount.ShouldBe(2, provider);
    }

    /// <summary>
    ///     Nothing hand-rolls the count: every builder a consumer can reach has to answer, so a new
    ///     one that forgets and inherits a wrong answer fails here rather than silently over-reporting
    ///     a budget to a caller that is trying to stay under it.
    /// </summary>
    [Fact]
    public void every_builder_in_every_provider_assembly_is_covered()
    {
        var covered = new[]
        {
            typeof(Postgresql.CommandBuilder), typeof(Postgresql.BatchBuilder),
            typeof(SqlServer.CommandBuilder), typeof(SqlServer.BatchBuilder),
            typeof(MySql.CommandBuilder), typeof(Sqlite.CommandBuilder),
            typeof(Weasel.Oracle.CommandBuilder), typeof(Weasel.Oracle.OracleDbCommandBuilder),
            typeof(Weasel.Firebird.CommandBuilder), typeof(Weasel.Firebird.FirebirdDbCommandBuilder)
        }.Select(x => x.FullName!).ToArray();

        var reachable = new[]
            {
                typeof(Postgresql.CommandBuilder).Assembly, typeof(SqlServer.CommandBuilder).Assembly,
                typeof(MySql.CommandBuilder).Assembly, typeof(Sqlite.CommandBuilder).Assembly,
                typeof(Weasel.Oracle.CommandBuilder).Assembly, typeof(Weasel.Firebird.CommandBuilder).Assembly
            }
            .SelectMany(x => x.GetExportedTypes())
            .Where(x => x is { IsAbstract: false, IsClass: true })
            .Where(x => typeof(ICommandBuilder).IsAssignableFrom(x) || typeof(DbCommandBuilder).IsAssignableFrom(x))
            .Select(x => x.FullName!)
            .OrderBy(x => x, StringComparer.Ordinal)
            .ToArray();

        reachable.Where(x => !covered.Contains(x)).ShouldBeEmpty(
            "every builder a consumer can reach needs a ParameterCount case above");
    }
}
