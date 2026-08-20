namespace Weasel.Core;

/// <summary>
///     The identifier checks every provider needs, factored out so each provider's
///     <see cref="Migrator.AssertValidIdentifier" /> only has to supply what is specific to it: the
///     characters its own dialect delimits with, its length limit, and the exception type it has always
///     thrown (weasel#416).
/// </summary>
/// <remarks>
///     <para>
///         <see cref="Migrator.AssertValidIdentifier" /> is the only identifier check in the stack --
///         <see cref="DbObjectName" /> and its provider-specific subclasses do no validation of their own --
///         and the migration path runs every schema object's name through it
///         (<c>DatabaseBase.ApplyAllConfiguredChangesToDatabaseAsync</c> and
///         <c>DatabaseBase.generateOrUpdateFeature</c>). So it has to reject the characters that let a name
///         escape the statement it is written into.
///     </para>
///     <para>
///         Two of those are the same for everyone. A <c>;</c> ends the statement and starts another. A
///         <c>'</c> closes a string literal, and object names do reach string literals on every provider --
///         the existence checks and introspection queries interpolate them (SQL Server's
///         <c>IF OBJECT_ID('...')</c>, Oracle's <c>WHERE table_name = '...'</c> inside an anonymous PL/SQL
///         block, SQLite's <c>pragma_table_info('...')</c>), and PostgreSQL writes a sequence's name into
///         one when a column defaults from it (<c>DEFAULT nextval('...')</c>). Whitespace is rejected
///         selectively: a line break or a tab can introduce a <c>--</c> comment into an unquoted
///         name and stays rejected, and leading or trailing whitespace is a typo every time, but a
///         plain interior space is somebody's real legacy column and is allowed through
///         (weasel#448).
///     </para>
///     <para>
///         <strong>This runs on the migration path only.</strong>
///         <c>DatabaseBase.ApplyAllConfiguredChangesToDatabaseAsync</c> and
///         <c>DatabaseBase.generateOrUpdateFeature</c> check every name a schema object will write
///         -- the objects it creates (<c>ISchemaObject.AllNames</c>) and the names that are not
///         objects of their own, columns, primary key and check constraints
///         (<c>ISchemaObjectWithLocalIdentifiers.LocalIdentifiers</c>). Calling a schema object's
///         <c>WriteCreateStatement</c> or <c>ApplyChangesAsync</c> directly does not go through
///         here at all. That split is deliberate: the direct API is how you drive a schema Weasel
///         did not author, and there the provider's quoting is what keeps a hostile name safe
///         (weasel#447). The migration path is where Weasel is choosing the names, and it is strict
///         about what it will bring into existence.
///     </para>
///     <para>
///         The rest is per-provider, because the character that closes an identifier is not: SQL Server
///         delimits with <c>[...]</c> as well as <c>"..."</c>, MySQL with backticks, and Oracle, SQLite and
///         PostgreSQL with <c>"</c>. Weasel's quoting helpers do not double an embedded delimiter (SQLite's
///         <c>SchemaUtils.QuoteName</c> does, but only quotes at all for keywords, spaces, dashes and
///         leading digits), so a name carrying one does not stay inside its own quotes.
///     </para>
/// </remarks>
public static class IdentifierValidation
{
    /// <summary>
    ///     Returns why <paramref name="name" /> is unsafe to write into DDL, phrased to follow "because",
    ///     or <c>null</c> when it passes. Length is deliberately not checked here -- the limit and the
    ///     exception that reports it differ per provider.
    /// </summary>
    /// <param name="name">The identifier to check. Null, empty and all-whitespace are rejected.</param>
    /// <param name="unsafeCharacters">
    ///     The characters that are unsafe for this provider on top of the universal ones: its identifier
    ///     delimiters, plus anything else that can break out of the context a name lands in -- MySQL's
    ///     backslash escape, for instance.
    /// </param>
    public static string? FindProblem(string? name, string unsafeCharacters)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return "it is null, empty, or entirely whitespace";
        }

        // Leading and trailing whitespace is always a typo, and allowing it would silently create
        // an object under a name nobody meant, which then drifts forever. Interior whitespace is a
        // different thing entirely -- "unit price" is somebody's real legacy column.
        if (name![0] == ' ' || char.IsWhiteSpace(name[0]) || char.IsWhiteSpace(name[^1]))
        {
            return "it starts or ends with whitespace";
        }

        foreach (var c in name)
        {
            if (char.IsWhiteSpace(c) && c != ' ')
            {
                // A newline or a tab can introduce a -- comment into an unquoted name; a plain
                // space cannot.
                return "it contains a line break or tab";
            }

            if (c is ';' or '\'' || unsafeCharacters.Contains(c))
            {
                return $"it contains {Describe(c)}";
            }
        }

        return null;
    }

    private static string Describe(char c)
    {
        return c switch
        {
            ';' => "a semicolon",
            '\'' => "a single quote",
            '"' => "a double quote",
            '`' => "a backtick",
            '[' => "an opening square bracket",
            ']' => "a closing square bracket",
            '\\' => "a backslash",
            _ => $"the character '{c}'"
        };
    }
}
