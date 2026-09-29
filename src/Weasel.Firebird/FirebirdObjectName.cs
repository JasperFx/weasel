using JasperFx.Core;
using Weasel.Core;

namespace Weasel.Firebird;

/// <summary>
///     A Firebird object name. Firebird before 6 has no schemas, so every object lives in the
///     pseudo-schema <see cref="DefaultSchema" /> -- the name Firebird 6 gives its default schema --
///     which is never written into DDL.
/// </summary>
/// <remarks>
///     <para>
///         This is SQLite's <c>main</c> model: the schema is carried so that the rest of Weasel, which
///         thinks in <c>schema.name</c>, has one to read, and the qualified name leaves it out.
///     </para>
///     <para>
///         A name in any other schema can still be constructed -- the conformance suites parse
///         <c>things.orders</c> and expect <c>things</c> back -- but it is refused at the DDL boundary,
///         by <see cref="AssertDefaultSchema" />, the moment anything tries to create it. Refusing here
///         instead would make the object model disagree with every other provider's about what a name
///         is.
///     </para>
/// </remarks>
public class FirebirdObjectName: DbObjectName
{
    /// <summary>
    ///     The pseudo-schema every Firebird object lives in.
    /// </summary>
    public const string DefaultSchema = "PUBLIC";

    protected override string QuotedQualifiedName => QualifiedName;

    /// <summary>
    ///     A name can arrive already delimited -- <c>QualifiedNameParser</c> keeps the parts of a
    ///     qualified name exactly as written. The model has to hold the spelling the catalog reports,
    ///     because that is what introspection binds; holding the delimited spelling matched nothing, so
    ///     the object read as absent and was recreated on every run (weasel#499).
    /// </summary>
    public FirebirdObjectName(string schema, string name)
        : base(SchemaUtils.Unquote(schema), SchemaUtils.Unquote(name),
            BuildQualifiedName(SchemaUtils.Unquote(schema), SchemaUtils.Unquote(name)))
    {
    }

    public FirebirdObjectName(string name): this(DefaultSchema, name)
    {
    }

    private FirebirdObjectName(DbObjectName dbObjectName): this(dbObjectName.Schema, dbObjectName.Name)
    {
    }

    public static FirebirdObjectName From(DbObjectName dbObjectName) =>
        dbObjectName as FirebirdObjectName ?? new FirebirdObjectName(dbObjectName);

    private static string BuildQualifiedName(string schema, string name)
    {
        return IsDefaultSchema(schema)
            ? SchemaUtils.QuoteName(name)
            : $"{SchemaUtils.QuoteName(schema)}.{SchemaUtils.QuoteName(name)}";
    }

    /// <summary>
    ///     Whether <paramref name="schema" /> is the pseudo-schema, in any case, or no schema at all.
    /// </summary>
    public static bool IsDefaultSchema(string? schema)
        => schema.IsEmpty() || schema!.Equals(DefaultSchema, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    ///     Refuse a schema other than <see cref="DefaultSchema" /> before it reaches DDL. Every
    ///     statement writer calls this, and so does the migrator before it runs anything, so the refusal
    ///     comes before the first statement rather than as a syntax error halfway through.
    /// </summary>
    /// <exception cref="NotSupportedException">The schema is not the default one.</exception>
    public static void AssertDefaultSchema(string? schema, string? subject = null)
    {
        if (IsDefaultSchema(schema))
        {
            return;
        }

        var what = subject.IsEmpty() ? $"schema '{schema}'" : $"{subject} in schema '{schema}'";
        throw new NotSupportedException(
            $"Cannot create {what}: Firebird 3, 4 and 5 have no schemas, so every object lives in the "
            + $"default one. Use the default schema ({DefaultSchema}), or leave the schema out.");
    }

    private new bool Equals(DbObjectName other)
    {
        return string.Equals(QualifiedName, other.QualifiedName, StringComparison.OrdinalIgnoreCase);
    }

    public override bool Equals(object? obj)
    {
        if (obj is DbObjectName dbObjectName)
        {
            return Equals(dbObjectName);
        }

        return base.Equals(obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            return (typeof(DbObjectName).GetHashCode() * 397) ^ QualifiedName.ToUpperInvariant().GetHashCode();
        }
    }
}
