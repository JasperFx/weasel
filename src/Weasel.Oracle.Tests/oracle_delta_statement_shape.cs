using Shouldly;
using Weasel.Core;
using Weasel.Oracle.Functions;
using Weasel.Oracle.Packages;
using Weasel.Oracle.Procedures;
using Weasel.Oracle.Synonyms;
using Weasel.Oracle.Triggers;
using Weasel.Oracle.Views;
using Xunit;

namespace Weasel.Oracle.Tests;

/// <summary>
///     No Oracle delta prefixes a drop to its create.
/// </summary>
/// <remarks>
///     <para>
///         Oracle has no <c>DROP … IF EXISTS</c> before 23c, so every drop in this provider is an
///         anonymous PL/SQL block that swallows the "does not exist" error. <c>OracleMigrator</c>
///         executes one command per statement, splitting on the <c>/</c> terminator — so a drop
///         block written immediately before a <c>CREATE OR REPLACE</c>, with no <c>/</c> between
///         them, reaches ODP.NET as a PL/SQL block followed by a DDL statement in one command, and
///         fails with <c>PLS-00103: Encountered the symbol "CREATE"</c>.
///     </para>
///     <para>
///         Six object types hit that separately and each grew its own delta class to avoid it. This
///         is the invariant those six were all encoding, asserted once, so the seventh does not
///         have to discover it: <see cref="OracleReplaceDelta" /> is what enforces it, and this is
///         what fails if a new object type quietly stops using it.
///     </para>
/// </remarks>
public class oracle_delta_statement_shape
{
    private const string Schema = "WEASEL";

    public static TheoryData<string, ISchemaObject> Objects =>
        new()
        {
            {
                "View",
                new View($"{Schema}.shape_view", $"select id from {Schema}.shape_src")
            },
            {
                "Trigger",
                new Trigger($"{Schema}.shape_trg", $"{Schema}.shape_src", "BEGIN NULL; END;")
                {
                    Timing = TriggerTiming.Before, Events = TriggerEvents.Insert
                }
            },
            {
                "StoredProcedure",
                new StoredProcedure($"{Schema}.shape_proc",
                    $"CREATE OR REPLACE PROCEDURE {Schema}.shape_proc AS BEGIN NULL; END;")
            },
            {
                "Function",
                new Function($"{Schema}.shape_fn",
                    $"CREATE OR REPLACE FUNCTION {Schema}.shape_fn RETURN NUMBER IS BEGIN RETURN 1; END;")
            },
            {
                "Synonym",
                new Synonym($"{Schema}.shape_syn", $"{Schema}.shape_src")
            },
            {
                "Package",
                new Package($"{Schema}.shape_pkg",
                    $"CREATE OR REPLACE PACKAGE {Schema}.shape_pkg AS PROCEDURE noop; END;",
                    $"CREATE OR REPLACE PACKAGE BODY {Schema}.shape_pkg AS PROCEDURE noop IS BEGIN NULL; END; END;")
            }
        };

    /// <summary>
    ///     Split the way <c>OracleMigrator.executeCommand</c> splits, so what is asserted is what
    ///     the server would actually be sent.
    /// </summary>
    private static string[] AsCommands(string sql)
        => sql.Split(["\n/\n", "\n/", "/\n"], StringSplitOptions.RemoveEmptyEntries)
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToArray();

    private static string UpdateSqlFor(ISchemaObject schemaObject)
    {
        var delta = new OracleReplaceDelta(schemaObject, SchemaPatchDifference.Update);

        var writer = new StringWriter();
        delta.WriteUpdate(new OracleMigrator(), writer);

        return writer.ToString();
    }

    /// <summary>
    ///     The failure this exists for: a command containing a PL/SQL drop block and then a CREATE.
    /// </summary>
    [Theory]
    [MemberData(nameof(Objects))]
    public void no_command_mixes_a_drop_block_with_a_create(string objectType, ISchemaObject schemaObject)
    {
        foreach (var command in AsCommands(UpdateSqlFor(schemaObject)))
        {
            var hasDropBlock = command.Contains("EXECUTE IMMEDIATE 'DROP", StringComparison.OrdinalIgnoreCase);
            var hasCreate = command.Contains("CREATE", StringComparison.OrdinalIgnoreCase);

            (hasDropBlock && hasCreate).ShouldBeFalse(
                $"{objectType}'s update puts a PL/SQL drop block and a CREATE in one command, which "
                + $"ODP.NET rejects with PLS-00103:\n{command}");
        }
    }

    [Theory]
    [MemberData(nameof(Objects))]
    public void an_update_emits_at_least_one_command(string objectType, ISchemaObject schemaObject)
    {
        AsCommands(UpdateSqlFor(schemaObject)).ShouldNotBeEmpty($"{objectType} produced no SQL to run");
    }

    /// <summary>
    ///     A create that spans more than one statement is fine as long as it separates them itself.
    ///     A package emits its specification, a <c>/</c>, and then its body — two commands, which is
    ///     exactly right, and the distinction this whole invariant turns on.
    /// </summary>
    [Fact]
    public void a_package_is_allowed_to_be_two_commands_because_it_separates_them()
    {
        var package = new Package($"{Schema}.shape_pkg",
            $"CREATE OR REPLACE PACKAGE {Schema}.shape_pkg AS PROCEDURE noop; END;",
            $"CREATE OR REPLACE PACKAGE BODY {Schema}.shape_pkg AS PROCEDURE noop IS BEGIN NULL; END; END;");

        AsCommands(UpdateSqlFor(package)).Length.ShouldBe(2);
    }

    /// <summary>
    ///     An object marked removed writes its drop instead, alone — a lone PL/SQL block, which is a
    ///     perfectly good command.
    /// </summary>
    [Fact]
    public void a_removed_object_writes_its_drop_and_nothing_else()
    {
        var procedure = new StoredProcedure($"{Schema}.shape_proc",
            $"CREATE OR REPLACE PROCEDURE {Schema}.shape_proc AS BEGIN NULL; END;") { IsRemoved = true };
        var delta = new OracleReplaceDelta(procedure, SchemaPatchDifference.Update, isRemoved: true);

        var writer = new StringWriter();
        delta.WriteUpdate(new OracleMigrator(), writer);

        var sql = writer.ToString();
        sql.ShouldContain("DROP PROCEDURE");
        sql.ShouldNotContain("CREATE");
    }
}
