using System.Text;
using JasperFx.Core;

namespace Weasel.Core;

/// <summary>
///     How one dialect delimits, escapes and normalizes identifiers. The five providers each grew
///     their own <c>SchemaUtils.QuoteName</c> in isolation and diverged sharply — one could not
///     escape its own delimiter, one conflated quoting with case folding, one quoted for a
///     hand-written list of characters. This is the shared behaviour, so only what is genuinely
///     dialect-specific stays per provider: the delimiter pair, what counts as a regular
///     identifier, and the keyword list.
/// </summary>
/// <remarks>
///     <para>
///         SQL Server's implementation is the reference — it is the one that has been worked over
///         (weasel#443, weasel#446) until it handled embedded delimiters, pre-delimited names and
///         string literals correctly, and this class is that behaviour lifted out.
///     </para>
///     <para>
///         Call sites stay on their provider's static <c>SchemaUtils</c> facade; that facade
///         delegates here. Nothing in the DDL writers has to change to adopt this.
///     </para>
/// </remarks>
public abstract class IdentifierRules
{
    /// <summary>The character that opens a delimited identifier — <c>[</c>, <c>"</c> or a backtick.</summary>
    protected abstract char Open { get; }

    /// <summary>
    ///     The character that closes a delimited identifier. Doubling this character is what escapes
    ///     it, in every dialect Weasel supports.
    /// </summary>
    protected abstract char Close { get; }

    /// <summary>Whether the name is one of this dialect's reserved words, and so needs delimiting.</summary>
    public abstract bool IsReservedWord(string name);

    /// <summary>
    ///     Whether the name can be written into DDL bare. The rule differs per dialect — which
    ///     characters may lead, which may follow — so each provider supplies its own.
    /// </summary>
    public abstract bool IsRegularIdentifier(string name);

    /// <summary>
    ///     Whether <see cref="Quote" /> should delimit this name. The default — anything that is not
    ///     a plain regular identifier, plus reserved words — suits most dialects. A provider
    ///     overrides it when it needs more: MySQL delimits unconditionally, and PostgreSQL also
    ///     delimits anything containing an uppercase character, because leaving it bare would let
    ///     the server fold the case and change which object the name refers to.
    /// </summary>
    public virtual bool RequiresDelimiting(string name)
        => !IsRegularIdentifier(name) || IsReservedWord(name);

    /// <summary>
    ///     Whether a name the caller has already delimited is passed through untouched rather than
    ///     escaped again. On by default, because Weasel emitted most identifiers bare until 9.25 and
    ///     delimiting the name yourself was the only way to use one that needed it — re-escaping
    ///     those would silently rename the object.
    /// </summary>
    /// <remarks>
    ///     The cost is that a name genuinely containing its own delimiters cannot be expressed:
    ///     <c>[x]</c> names the object <c>x</c>. That is the rarer case by a wide margin, but a
    ///     provider with no such legacy can turn the pass-through off and get the stricter reading.
    /// </remarks>
    protected virtual bool PassThroughDelimitedNames => true;

    /// <summary>
    ///     Delimit unconditionally, escaping any embedded close character. No pass-through — use
    ///     this only for a value that is the object's literal name and cannot have been delimited by
    ///     a caller, such as a database name read out of a connection string.
    /// </summary>
    public string Delimit(string name)
        => name.IsEmpty() ? name : $"{Open}{name.Replace(Close.ToString(), $"{Close}{Close}")}{Close}";

    /// <summary>
    ///     Delimit, unless the caller already did. Ordinary names come out exactly as
    ///     <see cref="Delimit" /> would leave them, so DDL for a site that has always delimited is
    ///     byte-identical.
    /// </summary>
    public string DelimitIfNeeded(string name)
        => name.IsEmpty() || (PassThroughDelimitedNames && IsDelimited(name)) ? name : Delimit(name);

    /// <summary>
    ///     Delimit only when the name cannot be written bare, so DDL for an ordinary schema is
    ///     unchanged. A name the caller already delimited is passed through.
    /// </summary>
    public string Quote(string name)
        => name.IsEmpty() || !RequiresDelimiting(name) || (PassThroughDelimitedNames && IsDelimited(name))
            ? name
            : Delimit(DelimitedForm(name));

    /// <summary>
    ///     The spelling a name takes once it has to be delimited. Identity for most dialects.
    /// </summary>
    /// <remarks>
    ///     Oracle folds an undelimited identifier to upper case, so a name that has to be delimited
    ///     — because it is reserved, or because of its shape — must be delimited in the folded
    ///     spelling to land on the same object it would have had bare. Delimiting
    ///     <c>order date</c> as <c>"order date"</c> would name a different column from the
    ///     <c>ORDER_DATE</c> everything else in the schema resolves to.
    /// </remarks>
    protected virtual string DelimitedForm(string name) => name;

    /// <summary>
    ///     Whether two spellings denote the same object once this dialect has resolved them.
    ///     Ordinal by default.
    /// </summary>
    /// <remarks>
    ///     Oracle folds, so <c>order date</c> and <c>ORDER DATE</c> are one object there and two
    ///     everywhere else. This is what lets "quoting must not change which object a name refers
    ///     to" be stated once and hold for a folding dialect as well as a preserving one — without
    ///     weakening it into a case-insensitive comparison for the providers that do preserve case.
    /// </remarks>
    public virtual bool SameObject(string a, string b) => string.Equals(a, b, StringComparison.Ordinal);

    /// <summary>
    ///     True when the name is delimited end to end with every interior close character already
    ///     doubled — exactly what <see cref="Delimit" /> produces, and nothing looser.
    /// </summary>
    /// <remarks>
    ///     The narrowness is what keeps the pass-through from being a hole. A name that merely
    ///     starts and ends with the delimiters does not qualify:
    ///     <c>[ix] ON t(id); DROP TABLE victim; --]</c> carries a lone <c>]</c>, so it is escaped on
    ///     its own terms and the DDL it is carrying stays inert inside one delimited identifier.
    /// </remarks>
    public bool IsDelimited(string name)
    {
        if (name.IsEmpty() || name.Length < 2 || name[0] != Open || name[^1] != Close)
        {
            return false;
        }

        var inner = name.Substring(1, name.Length - 2);
        return !inner.Replace($"{Close}{Close}", "").Contains(Close);
    }

    /// <summary>
    ///     Strip the delimiters off a name the caller delimited, undoubling any interior close
    ///     character. A name that is not properly delimited is returned as it is.
    /// </summary>
    /// <remarks>
    ///     The counterpart to the pass-through in <see cref="Quote" />, and the half that is easy to
    ///     miss. Emitting the caller's delimiters untouched still leaves the model holding the
    ///     delimited spelling, while the database reports the bare name — so the two never compare
    ///     equal and the object reports drift on every check. A provider normalizes names through
    ///     this as they arrive so that what the model holds is what the database will report.
    /// </remarks>
    public string Undelimit(string name)
        => IsDelimited(name)
            ? name.Substring(1, name.Length - 2).Replace($"{Close}{Close}", Close.ToString())
            : name;

    /// <summary>
    ///     Split a qualified name into its parts, on the dots that actually separate identifiers.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         A dot inside a delimited identifier is an ordinary character on every dialect Weasel
    ///         supports, so <c>"my.schema".things</c> is two parts and not three. Splitting on every dot
    ///         made such a name unparseable -- and it is not exotic:
    ///         <c>PK_dbo.__MigrationHistory</c> is the key EF6 gives its own history table
    ///         (weasel#501).
    ///     </para>
    ///     <para>
    ///         Delimiters are returned as they were written. Undelimiting is the caller's job, and the
    ///         provider ObjectName constructors already do it (weasel#499).
    ///     </para>
    /// </remarks>
    public string[] SplitQualifiedName(string qualifiedName)
    {
        if (qualifiedName.IsEmpty())
        {
            return [qualifiedName];
        }

        var parts = new List<string>();
        var current = new StringBuilder();
        var delimited = false;

        for (var i = 0; i < qualifiedName.Length; i++)
        {
            var c = qualifiedName[i];

            if (delimited)
            {
                if (c == Close)
                {
                    // A doubled close character is an escaped one, not the end of the identifier.
                    if (i + 1 < qualifiedName.Length && qualifiedName[i + 1] == Close)
                    {
                        current.Append(c).Append(c);
                        i++;
                        continue;
                    }

                    delimited = false;
                }

                current.Append(c);
                continue;
            }

            // Open is checked before Close, which matters for the dialects where they are the same
            // character: the delimited flag above is what decides which one it is.
            if (c == Open)
            {
                delimited = true;
                current.Append(c);
                continue;
            }

            if (c == '.')
            {
                parts.Add(current.ToString());
                current.Clear();
                continue;
            }

            current.Append(c);
        }

        parts.Add(current.ToString());

        return parts.ToArray();
    }

    /// <summary>
    ///     Escape a value being written into a SQL string literal, by doubling its single quotes.
    /// </summary>
    /// <remarks>
    ///     Object names reach string literals on every provider — existence checks and introspection
    ///     queries interpolate them, and PostgreSQL writes a sequence's name into one for a column
    ///     default. Delimiting is the wrong tool there: a literal is terminated by <c>'</c>, not by
    ///     the identifier delimiter.
    /// </remarks>
    public static string EscapeLiteral(string value)
        => value.IsEmpty() ? value : value.Replace("'", "''");
}
