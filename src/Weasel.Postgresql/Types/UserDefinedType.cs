using System.Data.Common;
using JasperFx.Core;
using Npgsql;
using Weasel.Core;
using DbCommandBuilder = Weasel.Core.DbCommandBuilder;

namespace Weasel.Postgresql.Types;

/// <summary>
///     A PostgreSQL user-defined type: an enum, a domain, or a composite.
/// </summary>
/// <remarks>
///     <para>
///         The three are one class because they are one object in the catalog — a row in
///         <c>pg_type</c> — and because what Weasel does with each is the same: create it, notice
///         when its definition moved, drop it. Modelling them separately would triple the surface to
///         express a distinction the catalog does not make.
///     </para>
///     <para>
///         <strong>Changing one is not an <c>ALTER</c>.</strong> A domain's constraint can be
///         altered, an enum can gain a label, but reordering an enum's labels or changing a
///         composite's fields cannot — and a type in use cannot be dropped at all, because columns
///         depend on it. So a changed type reports <see cref="SchemaPatchDifference.Invalid" />: it
///         needs a human, and pretending otherwise would drop a column's type out from under it.
///     </para>
/// </remarks>
public class UserDefinedType: SchemaObjectBase
{
    private UserDefinedType(DbObjectName identifier, string kind, string definition): base(identifier)
    {
        Kind = kind;
        Definition = definition;
    }

    /// <summary><c>ENUM</c>, <c>DOMAIN</c> or <c>COMPOSITE</c>.</summary>
    public string Kind { get; }

    /// <summary>
    ///     The type's contents as they will be written and as they come back from the catalog: the
    ///     label list, the base type and constraint, or the field list.
    /// </summary>
    public string Definition { get; }

    /// <summary>
    ///     An enum type over a fixed set of labels. Marten generates these, so there is an existing
    ///     consumer.
    /// </summary>
    public static UserDefinedType Enum(string name, params string[] labels)
        => new(DbObjectName.Parse(PostgresqlProvider.Instance, name), "ENUM",
            labels.Select(x => $"'{IdentifierRules.EscapeLiteral(x)}'").Join(", "));

    /// <summary>
    ///     A domain: a base type plus an optional constraint, reusable as a column type.
    /// </summary>
    public static UserDefinedType Domain(string name, string baseType, string? constraint = null)
        => new(DbObjectName.Parse(PostgresqlProvider.Instance, name), "DOMAIN",
            constraint.IsEmpty() ? baseType : $"{baseType} CHECK ({constraint})");

    /// <summary>
    ///     A composite type: named fields, usable as a column type or a function's return.
    /// </summary>
    public static UserDefinedType Composite(string name, params (string Name, string Type)[] fields)
        => new(DbObjectName.Parse(PostgresqlProvider.Instance, name), "COMPOSITE",
            fields.Select(x => $"{SchemaUtils.QuoteName(x.Name)} {x.Type}").Join(", "));

    public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
    {
        writer.WriteLine(Kind switch
        {
            "ENUM" => $"CREATE TYPE {Identifier} AS ENUM ({Definition});",
            "DOMAIN" => $"CREATE DOMAIN {Identifier} AS {Definition};",
            _ => $"CREATE TYPE {Identifier} AS ({Definition});"
        });
    }

    public override void WriteDropStatement(Migrator rules, TextWriter writer)
    {
        // No CASCADE: a type with dependents should refuse to be dropped rather than take the
        // columns that use it.
        writer.WriteLine(Kind == "DOMAIN"
            ? $"DROP DOMAIN IF EXISTS {Identifier};"
            : $"DROP TYPE IF EXISTS {Identifier};");
    }

    public override void ConfigureQueryCommand(DbCommandBuilder builder)
    {
        var nameParam = builder.AddParameter(Identifier.Name).ParameterName;
        var schemaParam = builder.AddParameter(Identifier.Schema).ParameterName;

        // One query for all three kinds: the label list for an enum, the base type and constraint
        // for a domain, the field list for a composite.
        builder.Append($@"
SELECT
    CASE
        WHEN t.typtype = 'e' THEN (
            SELECT string_agg(quote_literal(e.enumlabel), ', ' ORDER BY e.enumsortorder)
            FROM pg_enum e WHERE e.enumtypid = t.oid)
        WHEN t.typtype = 'd' THEN
            format_type(t.typbasetype, t.typtypmod) ||
            COALESCE((SELECT ' ' || pg_get_constraintdef(c.oid)
                      FROM pg_constraint c WHERE c.contypid = t.oid LIMIT 1), '')
        ELSE (
            SELECT string_agg(quote_ident(a.attname) || ' ' || format_type(a.atttypid, a.atttypmod), ', '
                              ORDER BY a.attnum)
            FROM pg_attribute a WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped)
    END
FROM pg_type t
JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE t.typname = :{nameParam} AND n.nspname = :{schemaParam}
  AND t.typtype IN ('e', 'd', 'c')");
    }

    public override async Task<ISchemaObjectDelta> CreateDeltaAsync(DbDataReader reader, CancellationToken ct = default)
    {
        var existing = await readDefinitionAsync(reader, ct).ConfigureAwait(false);

        if (existing == null)
        {
            return new SchemaObjectDelta(this, SchemaPatchDifference.Create);
        }

        return Normalize(existing) == Normalize(Definition)
            ? new SchemaObjectDelta(this, SchemaPatchDifference.None)
            : new SchemaObjectDelta(this, SchemaPatchDifference.Invalid);
    }

    /// <summary>
    ///     PostgreSQL renders what it stores — <c>varchar</c> becomes
    ///     <c>character varying</c>, a check constraint comes back with its own parenthesization —
    ///     so the comparison ignores whitespace and case. It is not exact, and a purely cosmetic
    ///     difference in a domain's constraint can read as a change; reporting that is
    ///     <see cref="SchemaPatchDifference.Invalid" /> means a human looks at it rather than a
    ///     migration dropping a type some column depends on.
    /// </summary>
    internal static string Normalize(string definition)
    {
        // PostgreSQL renders type names by their canonical spelling, so a composite declared with
        // varchar(100) comes back as character varying(100); route both sides through the provider's
        // own synonym table rather than keeping a second list here.
        var canonical = TypeName.Replace(definition, m =>
            PostgresqlProvider.Instance.ConvertSynonyms(m.Value.ToLowerInvariant()));

        return canonical
            .Replace(" ", "").Replace("\"", "").Replace("\r", "").Replace("\n", "")
            // pg_get_constraintdef parenthesizes a domain's check where the caller may not have.
            .Replace("(", "").Replace(")", "")
            .Trim().ToUpperInvariant();
    }

    private static readonly System.Text.RegularExpressions.Regex TypeName =
        new(@"character varying|timestamp without time zone|timestamp with time zone|double precision",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase
            | System.Text.RegularExpressions.RegexOptions.Compiled);

    public async Task<string?> FetchExistingDefinitionAsync(NpgsqlConnection conn, CancellationToken ct = default)
    {
        var builder = new DbCommandBuilder(conn);
        ConfigureQueryCommand(builder);

        await using var reader = await conn.ExecuteReaderAsync(builder, ct).ConfigureAwait(false);
        var definition = await readDefinitionAsync(reader, ct).ConfigureAwait(false);
        await reader.CloseAsync().ConfigureAwait(false);

        return definition;
    }

    public async Task<bool> ExistsInDatabaseAsync(NpgsqlConnection conn, CancellationToken ct = default)
        => await FetchExistingDefinitionAsync(conn, ct).ConfigureAwait(false) != null;

    private static async Task<string?> readDefinitionAsync(DbDataReader reader, CancellationToken ct)
    {
        if (!await reader.ReadAsync(ct).ConfigureAwait(false)) return null;
        if (await reader.IsDBNullAsync(0, ct).ConfigureAwait(false)) return null;

        var definition = await reader.GetFieldValueAsync<string>(0, ct).ConfigureAwait(false);
        return definition.IsEmpty() ? null : definition;
    }
}
