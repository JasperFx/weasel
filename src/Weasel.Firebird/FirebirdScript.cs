using System.Text;
using JasperFx.Core;

namespace Weasel.Firebird;

/// <summary>
///     Writes and splits Firebird DDL in the form isql runs: a plain statement ends in <c>;</c>, and a
///     PSQL statement -- an <c>EXECUTE BLOCK</c>, a procedure, a trigger -- is wrapped in
///     <c>SET TERM ^ ;</c> … <c>^</c> … <c>SET TERM ; ^</c>, because its body is full of semicolons.
/// </summary>
/// <remarks>
///     <para>
///         Every statement Weasel.Firebird renders goes through <see cref="WriteStatement" /> or
///         <see cref="WritePsql" />, so everything it renders splits back with <see cref="Split" />:
///         that is how a migration is executed one statement per command -- Firebird refuses two in
///         one -- and how a <c>db-patch</c> script stays runnable in isql unchanged.
///     </para>
///     <para>
///         <see cref="Split" /> is a lexer, not a regular expression. It knows string literals
///         (<c>'…'</c> with <c>''</c> escapes, and Firebird's <c>q'{…}'</c> form), delimited
///         identifiers (<c>"…"</c>), and both comment forms, so a <c>;</c> or a <c>^</c> inside any of
///         them never ends a statement. <c>SET TERM</c> is recognised only where a statement starts,
///         which is where isql recognises it.
///     </para>
///     <para>
///         FirebirdClient ships a splitter of its own, <c>FbScript</c>. It is not used here: it hands
///         back statement text with the leading comments still attached, throws on statement kinds it
///         cannot classify, and lags the server's syntax -- it rejects <c>q'{…}'</c> literals,
///         <c>CREATE OR ALTER SEQUENCE</c> and <c>RECREATE SEQUENCE</c>, all of which the server accepts.
///     </para>
/// </remarks>
public static class FirebirdScript
{
    /// <summary>
    ///     The terminator a PSQL statement ends with, between <c>SET TERM ^ ;</c> and
    ///     <c>SET TERM ; ^</c>.
    /// </summary>
    public const string PsqlTerminator = "^";

    /// <summary>
    ///     The longest single piece a string literal is written in inside an <c>EXECUTE STATEMENT</c>.
    ///     A UTF8 attachment caps one literal at 16,383 characters (and 65,535 bytes, which 16,000
    ///     four-byte characters stay under); longer DDL is written as several literals joined with
    ///     <c>||</c>, which the server accepts to well past 40,000 characters.
    /// </summary>
    public const int MaxLiteralPieceLength = 16000;

    /// <summary>
    ///     The statements of <paramref name="script" />, in order, without their terminators and
    ///     trimmed of surrounding whitespace. <c>SET TERM</c> directives are consumed and fragments that
    ///     hold nothing but comments are dropped; everything else is verbatim, leading comments included.
    /// </summary>
    public static IReadOnlyList<string> Split(string script) => Parse(script).Select(x => x.Text).ToArray();

    /// <summary>
    ///     Write a plain statement, terminated with <c>;</c>.
    /// </summary>
    /// <exception cref="ArgumentException">
    ///     The statement is empty, or has a <c>;</c> outside a literal or comment -- which would end it
    ///     early, so it is either two statements or PSQL that belongs in <see cref="WritePsql" />.
    /// </exception>
    public static void WriteStatement(TextWriter writer, string statement)
    {
        var text = statement.Trim();
        if (text.EndsWith(';'))
        {
            text = text[..^1].TrimEnd();
        }

        if (text.IsEmpty())
        {
            throw new ArgumentException("A statement cannot be empty.", nameof(statement));
        }

        if (FindOutsideLiterals(text, ";") >= 0)
        {
            throw new ArgumentException(
                $"A plain statement cannot contain ';' outside a literal or comment, because isql would end the statement there. Write PSQL through {nameof(WritePsql)}: {text}",
                nameof(statement));
        }

        writer.Write(text);
        writer.WriteLine(";");
    }

    /// <summary>
    ///     Write a PSQL statement, wrapped in <c>SET TERM ^ ;</c> … <c>^</c> … <c>SET TERM ; ^</c>.
    /// </summary>
    /// <exception cref="ArgumentException">
    ///     The statement is empty, or has a <c>^</c> outside a literal or comment. isql would end the
    ///     statement at it, which truncates the body silently rather than failing -- so it is refused
    ///     when it is written (weasel#624). Firebird accepts <c>^=</c> for "not equal", which is the usual
    ///     way one gets in: write <c>&lt;&gt;</c> instead.
    /// </exception>
    public static void WritePsql(TextWriter writer, string statement)
    {
        var text = statement.Trim();
        if (text.IsEmpty())
        {
            throw new ArgumentException("A PSQL statement cannot be empty.", nameof(statement));
        }

        var caret = FindOutsideLiterals(text, PsqlTerminator);
        if (caret >= 0)
        {
            throw new ArgumentException(
                $"A PSQL statement cannot contain '^' outside a literal or comment, because '^' ends a statement in isql and would truncate the body there. Write '<>' for 'not equal' rather than '^='. The '^' is at position {caret} of: {text}",
                nameof(statement));
        }

        writer.WriteLine("SET TERM ^ ;");
        writer.WriteLine(text);
        writer.WriteLine(PsqlTerminator);
        writer.WriteLine("SET TERM ; ^");
    }

    /// <summary>
    ///     Rewrite rendered DDL in the form isql runs safely: every statement followed by
    ///     <c>COMMIT;</c>.
    /// </summary>
    /// <remarks>
    ///     isql runs a script in one transaction until it meets a <c>COMMIT</c>, and Firebird applies DDL
    ///     at commit. A guarded <c>EXECUTE BLOCK</c> and a plain DDL statement in the same transaction
    ///     then fail together at that commit (SQLSTATE 40001), and the guarded object is silently lost.
    ///     A commit after every statement is what each statement gets when a migration is applied, so a
    ///     script and an apply behave alike.
    /// </remarks>
    public static void WriteIsqlScript(TextWriter writer, string script)
    {
        foreach (var statement in Parse(script))
        {
            if (IsCommit(statement.Text))
            {
                continue;
            }

            if (statement.IsPsql)
            {
                WritePsql(writer, statement.Text);
            }
            else
            {
                WriteStatement(writer, statement.Text);
            }

            writer.WriteLine("COMMIT;");
        }
    }

    /// <summary>
    ///     Write an <c>EXECUTE BLOCK</c> that runs <paramref name="statement" /> only when
    ///     <paramref name="probe" /> finds nothing, which makes a <c>CREATE</c> or <c>ADD</c> safe to run
    ///     again and safe to race.
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="probe">A query that returns a row when the object already exists.</param>
    /// <param name="statement">The DDL to run, unescaped.</param>
    public static void WriteGuarded(TextWriter writer, string probe, string statement)
        => WritePsql(writer, Guarded(probe, statement));

    /// <summary>
    ///     Write an <c>EXECUTE BLOCK</c> that runs <paramref name="statement" /> only when
    ///     <paramref name="probe" /> finds something -- the mirror of <see cref="WriteGuarded" />, for a
    ///     <c>DROP</c>.
    /// </summary>
    public static void WriteGuardedWhenExists(TextWriter writer, string probe, string statement)
        => WritePsql(writer, Guarded(probe, statement, whenExists: true));

    /// <summary>
    ///     The <c>EXECUTE BLOCK</c> behind <see cref="WriteGuarded" /> and
    ///     <see cref="WriteGuardedWhenExists" />, unterminated.
    /// </summary>
    /// <remarks>
    ///     This is the one place DDL is written into a string literal, and so the one place its quotes
    ///     are doubled (weasel#643): a column default of <c>'x'</c> would otherwise end the literal early.
    ///     The probe compares <c>RDB$…_NAME = '…'</c> directly rather than through <c>TRIM</c>, so the
    ///     catalog's index is used; the columns are blank-padded <c>CHAR</c>, which compares equal to the
    ///     unpadded literal.
    /// </remarks>
    public static string Guarded(string probe, string statement, bool whenExists = false)
    {
        var builder = new StringBuilder();
        builder.Append("EXECUTE BLOCK AS").Append('\n');
        builder.Append("BEGIN").Append('\n');
        builder.Append("  IF (").Append(whenExists ? "" : "NOT ").Append("EXISTS(").Append(probe.Trim()).Append(")) THEN")
            .Append('\n');
        builder.Append("    EXECUTE STATEMENT ").Append(StatementLiteral(statement)).Append(';').Append('\n');
        builder.Append("END");

        return builder.ToString();
    }

    /// <summary>
    ///     A string literal holding <paramref name="value" />, with its quotes doubled.
    /// </summary>
    public static string Literal(string value) => $"'{SchemaUtils.EscapeLiteral(value)}'";

    /// <summary>
    ///     <paramref name="statement" /> as the argument of <c>EXECUTE STATEMENT</c>: one literal, or --
    ///     past <see cref="MaxLiteralPieceLength" /> characters -- several joined with <c>||</c>.
    /// </summary>
    /// <remarks>
    ///     The text is cut before it is escaped, so a doubled quote is never split across two pieces,
    ///     and never between the halves of a surrogate pair.
    /// </remarks>
    public static string StatementLiteral(string statement)
    {
        var text = statement.Trim();
        if (text.Length <= MaxLiteralPieceLength)
        {
            return Literal(text);
        }

        var pieces = new List<string>();
        var start = 0;
        while (start < text.Length)
        {
            var length = Math.Min(MaxLiteralPieceLength, text.Length - start);
            if (start + length < text.Length && char.IsHighSurrogate(text[start + length - 1]))
            {
                length--;
            }

            pieces.Add(Literal(text.Substring(start, length)));
            start += length;
        }

        return pieces.Join(" || ");
    }

    /// <summary>
    ///     The statements of <paramref name="script" /> and whether each was written as PSQL, which is
    ///     what <see cref="WriteIsqlScript" /> needs to write it back.
    /// </summary>
    internal static IReadOnlyList<ScriptStatement> Parse(string script)
    {
        var statements = new List<ScriptStatement>();
        if (script.IsEmpty())
        {
            return statements;
        }

        var terminator = ";";
        var position = 0;

        while (position < script.Length)
        {
            var start = position;
            var significant = skipInsignificant(script, position);

            if (significant >= script.Length)
            {
                // Nothing but whitespace and comments to the end: a comment-only fragment.
                break;
            }

            if (tryReadSetTerm(script, significant, terminator, out var newTerminator, out var afterDirective))
            {
                terminator = newTerminator;
                position = afterDirective;
                continue;
            }

            var end = findTerminator(script, significant, terminator);
            var text = script[start..(end < 0 ? script.Length : end)].Trim();

            if (hasSignificantText(text))
            {
                statements.Add(new ScriptStatement(text, terminator != ";"));
            }

            position = end < 0 ? script.Length : end + terminator.Length;
        }

        return statements;
    }

    /// <summary>
    ///     The position of <paramref name="token" /> in <paramref name="sql" /> outside any literal,
    ///     delimited identifier or comment, or -1.
    /// </summary>
    internal static int FindOutsideLiterals(string sql, string token) => findTerminator(sql, 0, token);

    /// <summary>
    ///     Is <paramref name="statement" /> an <c>EXECUTE BLOCK</c> -- which is what every guarded
    ///     statement is -- once any leading whitespace and comments are passed over?
    /// </summary>
    internal static bool IsExecuteBlock(string statement) => skipWords(statement, 0, "EXECUTE", "BLOCK") >= 0;

    /// <summary>
    ///     Is <paramref name="statement" /> <c>COMMIT</c> or <c>COMMIT WORK</c> and nothing else, comments
    ///     aside?
    /// </summary>
    internal static bool IsCommit(string statement)
    {
        var position = skipWords(statement, 0, "COMMIT");
        if (position < 0)
        {
            return false;
        }

        var afterWork = skipWords(statement, position, "WORK");
        return skipInsignificant(statement, afterWork < 0 ? position : afterWork) >= statement.Length;
    }

    /// <summary>
    ///     The position just past <paramref name="words" />, read in order from
    ///     <paramref name="position" /> with whitespace and comments allowed before each, or -1 when the
    ///     text does not read them.
    /// </summary>
    private static int skipWords(string sql, int position, params string[] words)
    {
        foreach (var word in words)
        {
            position = skipInsignificant(sql, position);
            if (!startsWithWord(sql, position, word))
            {
                return -1;
            }

            position += word.Length;
        }

        return position;
    }

    private static bool hasSignificantText(string text) => skipInsignificant(text, 0) < text.Length;

    /// <summary>
    ///     The first position at or after <paramref name="position" /> that is not whitespace or inside
    ///     a comment.
    /// </summary>
    private static int skipInsignificant(string sql, int position)
    {
        while (position < sql.Length)
        {
            if (char.IsWhiteSpace(sql[position]))
            {
                position++;
            }
            else if (startsWith(sql, position, "--"))
            {
                position = endOfLineComment(sql, position);
            }
            else if (startsWith(sql, position, "/*"))
            {
                position = endOfBlockComment(sql, position);
            }
            else
            {
                break;
            }
        }

        return position;
    }

    /// <summary>
    ///     <c>SET TERM &lt;new&gt; &lt;current&gt;</c>, recognised only at the start of a statement. The new
    ///     terminator is whatever stands between <c>TERM</c> and the current terminator, so both
    ///     <c>SET TERM ^ ;</c> and <c>SET TERM ^;</c> read the same.
    /// </summary>
    private static bool tryReadSetTerm(string sql, int position, string terminator, out string newTerminator,
        out int afterDirective)
    {
        newTerminator = terminator;
        afterDirective = position;

        if (!startsWithWord(sql, position, "SET"))
        {
            return false;
        }

        var next = skipWhitespace(sql, position + 3);
        if (next == position + 3 || !startsWithWord(sql, next, "TERM"))
        {
            return false;
        }

        var argumentStart = next + 4;
        if (argumentStart >= sql.Length || !char.IsWhiteSpace(sql[argumentStart]))
        {
            return false;
        }

        var end = sql.IndexOf(terminator, argumentStart, StringComparison.Ordinal);
        if (end < 0)
        {
            return false;
        }

        var argument = sql[argumentStart..end].Trim();
        if (argument.IsEmpty() || argument.Any(char.IsWhiteSpace))
        {
            return false;
        }

        newTerminator = argument;
        afterDirective = end + terminator.Length;
        return true;
    }

    /// <summary>
    ///     The position of the next <paramref name="terminator" /> at or after
    ///     <paramref name="position" /> that is outside every literal, delimited identifier and comment,
    ///     or -1.
    /// </summary>
    private static int findTerminator(string sql, int position, string terminator)
    {
        while (position < sql.Length)
        {
            var c = sql[position];

            if (startsWith(sql, position, "--"))
            {
                position = endOfLineComment(sql, position);
            }
            else if (startsWith(sql, position, "/*"))
            {
                position = endOfBlockComment(sql, position);
            }
            else if (c == '\'')
            {
                position = endOfQuoted(sql, position, '\'');
            }
            else if (c == '"')
            {
                position = endOfQuoted(sql, position, '"');
            }
            else if (c is 'q' or 'Q' && position + 2 < sql.Length && sql[position + 1] == '\''
                     && (position == 0 || !isIdentifierCharacter(sql[position - 1])))
            {
                position = endOfAlternativeLiteral(sql, position);
            }
            else if (startsWith(sql, position, terminator))
            {
                return position;
            }
            else
            {
                position++;
            }
        }

        return -1;
    }

    /// <summary>
    ///     The position just past a <c>'…'</c> literal or a <c>"…"</c> identifier opening at
    ///     <paramref name="position" />, where a doubled delimiter is an escaped one.
    /// </summary>
    private static int endOfQuoted(string sql, int position, char delimiter)
    {
        position++;
        while (position < sql.Length)
        {
            if (sql[position] == delimiter)
            {
                if (position + 1 < sql.Length && sql[position + 1] == delimiter)
                {
                    position += 2;
                    continue;
                }

                return position + 1;
            }

            position++;
        }

        return sql.Length;
    }

    /// <summary>
    ///     The position just past a <c>q'&lt;open&gt;…&lt;close&gt;'</c> literal. The closing character is
    ///     the partner of an opening bracket -- <c>(</c> <c>[</c> <c>{</c> <c>&lt;</c> -- or the opening
    ///     character itself.
    /// </summary>
    private static int endOfAlternativeLiteral(string sql, int position)
    {
        var open = sql[position + 2];
        var close = open switch
        {
            '(' => ')',
            '[' => ']',
            '{' => '}',
            '<' => '>',
            _ => open
        };

        var end = sql.IndexOf($"{close}'", position + 3, StringComparison.Ordinal);
        return end < 0 ? sql.Length : end + 2;
    }

    private static int endOfLineComment(string sql, int position)
    {
        var end = sql.IndexOf('\n', position);
        return end < 0 ? sql.Length : end + 1;
    }

    private static int endOfBlockComment(string sql, int position)
    {
        var end = sql.IndexOf("*/", position + 2, StringComparison.Ordinal);
        return end < 0 ? sql.Length : end + 2;
    }

    private static int skipWhitespace(string sql, int position)
    {
        while (position < sql.Length && char.IsWhiteSpace(sql[position]))
        {
            position++;
        }

        return position;
    }

    private static bool startsWith(string sql, int position, string value)
        => string.CompareOrdinal(sql, position, value, 0, value.Length) == 0;

    private static bool startsWithWord(string sql, int position, string word)
    {
        if (position + word.Length > sql.Length
            || string.Compare(sql, position, word, 0, word.Length, StringComparison.OrdinalIgnoreCase) != 0)
        {
            return false;
        }

        var after = position + word.Length;
        return after == sql.Length || !isIdentifierCharacter(sql[after]);
    }

    private static bool isIdentifierCharacter(char c) => char.IsLetterOrDigit(c) || c == '_' || c == '$';

    /// <summary>
    ///     One statement of a script, and whether it was terminated by something other than <c>;</c> --
    ///     which is to say, written as PSQL.
    /// </summary>
    internal readonly record struct ScriptStatement(string Text, bool IsPsql);
}
