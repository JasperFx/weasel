using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     Every identifier a table writes into its DDL has to be reachable by the migration path's
///     validation, and there are only two ways to reach one: <see cref="ISchemaObject.AllNames" />
///     for the named objects the table creates, and
///     <see cref="ISchemaObjectWithLocalIdentifiers.LocalIdentifiers" /> for the names that are not
///     objects of their own. This suite holds the floor that the union of the two covers
///     everything (weasel#448).
/// </summary>
/// <remarks>
///     <para>
///         Before this, four of the five providers yielded the table, its indexes and its foreign
///         keys, SQLite omitted foreign keys, and <em>no</em> provider yielded a column name, the
///         primary key constraint name, or a check constraint name. Those went into DDL unexamined.
///     </para>
///     <para>
///         The suite is built entirely through <see cref="ITable" />, so a provider joins with one
///         entry in <see cref="Providers" /> and nothing else. Pure model construction — no
///         connection, no container.
///     </para>
///     <para>
///         Names are compared case-insensitively throughout: Oracle folds an unquoted identifier to
///         uppercase on the way into the model, and that is correct behaviour rather than a
///         coverage gap.
///     </para>
/// </remarks>
public class table_identifier_coverage_conformance
{
    /// <summary>
    ///     The third value is whether the provider emits check constraints. Oracle, MySQL and
    ///     SQLite refuse them rather than accepting one they will not write (weasel#488), so there
    ///     is no check constraint name for them to validate — the absence is correct rather than a
    ///     coverage gap, and saying so here keeps it visible.
    /// </summary>
    public static TheoryData<string, Func<ITable>, bool> Providers =>
        new()
        {
            { "SqlServer", () => new SqlServer.Tables.Table("dbo.people"), true },
            { "MySql", () => new MySql.Tables.Table("weasel_testing.people"), false },
            { "Sqlite", () => new Sqlite.Tables.Table("people"), false },
            { "Postgresql", () => new Postgresql.Tables.Table("public.people"), true },
            { "Oracle", () => new Oracle.Tables.Table("WEASEL.PEOPLE"), false }
        };

    /// <summary>
    ///     One table carrying every kind of name a table can write: an identifier, a primary key
    ///     over a named column, an ordinary column, an index, a foreign key, and a check
    ///     constraint. Built through <see cref="ITable" /> so it is the same table on all five.
    /// </summary>
    private static ITable BuildFullyDressedTable(Func<ITable> factory, bool emitsCheckConstraints,
        out string[] expected)
    {
        var table = factory();

        table.AddPrimaryKeyColumn("id", typeof(int));
        table.AddColumn("email", typeof(string));
        table.AddColumn("state_id", typeof(int));

        table.PrimaryKeyName = "pk_people_id";
        table.AddIndex("idx_people_email", ["email"]);
        table.AddForeignKey("fk_people_state", table.Identifier, ["state_id"], ["id"]);

        var names = new List<string>
        {
            table.Identifier.Name,
            "id",
            "email",
            "state_id",
            "pk_people_id",
            "idx_people_email",
            "fk_people_state"
        };

        if (emitsCheckConstraints)
        {
            table.AddCheckConstraint("ck_people_email_present", "email is not null");
            names.Add("ck_people_email_present");
        }

        expected = names.ToArray();

        return table;
    }

    private static IEnumerable<string> EveryValidatedName(ITable table)
    {
        foreach (var name in table.AllNames()) yield return name.Name;

        if (table is ISchemaObjectWithLocalIdentifiers withLocals)
        {
            foreach (var name in withLocals.LocalIdentifiers()) yield return name;
        }
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void every_name_the_table_writes_is_reachable_by_validation(
        string provider, Func<ITable> factory, bool emitsCheckConstraints)
    {
        var table = BuildFullyDressedTable(factory, emitsCheckConstraints, out var expected);
        var validated = EveryValidatedName(table).ToArray();

        foreach (var name in expected)
        {
            validated.ShouldContain(
                x => string.Equals(x, name, StringComparison.OrdinalIgnoreCase),
                $"{provider} never offers '{name}' for validation, so it reaches DDL unchecked");
        }
    }

    /// <summary>
    ///     Index and foreign key names are real named objects, so they belong in
    ///     <see cref="ISchemaObject.AllNames" /> rather than in the local names. SQLite writes its
    ///     foreign keys inline in CREATE TABLE and had left them out entirely.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void all_names_carries_the_table_its_indexes_and_its_foreign_keys(
        string provider, Func<ITable> factory, bool emitsCheckConstraints)
    {
        var table = BuildFullyDressedTable(factory, emitsCheckConstraints, out _);
        var names = table.AllNames().Select(x => x.Name).ToArray();

        names.ShouldContain(x => string.Equals(x, table.Identifier.Name, StringComparison.OrdinalIgnoreCase),
            $"{provider} omits the table itself");
        names.ShouldContain(x => string.Equals(x, "idx_people_email", StringComparison.OrdinalIgnoreCase),
            $"{provider} omits the index name");
        names.ShouldContain(x => string.Equals(x, "fk_people_state", StringComparison.OrdinalIgnoreCase),
            $"{provider} omits the foreign key name");
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void local_identifiers_carry_the_names_that_are_not_objects(
        string provider, Func<ITable> factory, bool emitsCheckConstraints)
    {
        var table = BuildFullyDressedTable(factory, emitsCheckConstraints, out _);
        var locals = ((ISchemaObjectWithLocalIdentifiers)table).LocalIdentifiers().ToArray();

        locals.ShouldContain(x => string.Equals(x, "email", StringComparison.OrdinalIgnoreCase),
            $"{provider} omits a column name");
        locals.ShouldContain(x => string.Equals(x, "pk_people_id", StringComparison.OrdinalIgnoreCase),
            $"{provider} omits the primary key constraint name");

        if (emitsCheckConstraints)
        {
            locals.ShouldContain(x => string.Equals(x, "ck_people_email_present", StringComparison.OrdinalIgnoreCase),
                $"{provider} omits a check constraint name");
        }
    }

    /// <summary>
    ///     A table with no primary key never emits a primary key constraint, so validating the
    ///     name <see cref="ITable.PrimaryKeyName" /> falls back to would be checking a string that
    ///     is never written. Every provider derives that fallback from the table name, which has
    ///     already been validated.
    /// </summary>
    [Theory]
    [MemberData(nameof(Providers))]
    public void a_table_without_a_primary_key_offers_no_primary_key_name(
        string provider, Func<ITable> factory, bool emitsCheckConstraints)
    {
        var table = factory();
        table.AddColumn("email", typeof(string));

        var locals = ((ISchemaObjectWithLocalIdentifiers)table).LocalIdentifiers().ToArray();

        locals.ShouldNotContain(
            x => string.Equals(x, table.PrimaryKeyName, StringComparison.OrdinalIgnoreCase),
            $"{provider} offers a primary key name for a table that has no primary key");
    }
}
