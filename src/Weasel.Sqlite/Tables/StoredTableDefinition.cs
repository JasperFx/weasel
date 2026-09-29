using System.Text;

namespace Weasel.Sqlite.Tables;

/// <summary>
///     The column definitions and table constraints of a <c>CREATE TABLE</c> statement as SQLite
///     stores it in <c>sqlite_master.sql</c>, each one exactly as it was written.
/// </summary>
/// <remarks>
///     <para>
///         SQLite keeps the text it was given, and <c>ALTER TABLE ADD COLUMN</c> splices the new
///         column's definition into it after the last column. So this is the one place a column's
///         whole definition survives: its collation, a column <c>CHECK</c>, a generated expression,
///         an inline <c>REFERENCES</c>. No pragma reports any of those. A table rebuild that has to
///         put back something the model does not declare reads it from here, the way it already
///         reads the table's triggers back from <c>sqlite_master</c> (weasel#452).
///     </para>
///     <para>
///         Splitting follows SQLite's own tokenizer as far as the split needs it: a comma or a
///         parenthesis inside a string literal, a delimited name (<c>"…"</c>, <c>[…]</c>,
///         <c>`…`</c>) or a comment belongs to the definition it sits in, and only a comma at the
///         top level of the column list ends one. Comments between definitions are dropped.
///     </para>
/// </remarks>
internal sealed class StoredTableDefinition
{
    private static readonly HashSet<string> TableConstraintKeywords =
        new(StringComparer.OrdinalIgnoreCase) { "CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN" };

    private StoredTableDefinition(IReadOnlyList<StoredColumnDefinition> columns,
        IReadOnlyList<StoredTableConstraint> constraints)
    {
        Columns = columns;
        Constraints = constraints;
    }

    public IReadOnlyList<StoredColumnDefinition> Columns { get; }

    public IReadOnlyList<StoredTableConstraint> Constraints { get; }

    public StoredColumnDefinition? ColumnNamed(string name)
        => Columns.FirstOrDefault(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

    /// <summary>
    ///     Null when there is no statement, or when it has no complete column list to split.
    /// </summary>
    public static StoredTableDefinition? Parse(string? createTableSql)
    {
        if (string.IsNullOrWhiteSpace(createTableSql))
        {
            return null;
        }

        var tokens = tokenize(createTableSql);
        var open = tokens.FindIndex(x => x.Kind == TokenKind.Open);
        if (open < 0)
        {
            return null;
        }

        var parts = new List<List<Token>>();
        var current = new List<Token>();
        var depth = 0;
        var closed = false;

        for (var i = open + 1; i < tokens.Count && !closed; i++)
        {
            var token = tokens[i];
            switch (token.Kind)
            {
                case TokenKind.Open:
                    depth++;
                    break;

                case TokenKind.Close when depth == 0:
                    parts.Add(current);
                    closed = true;
                    continue;

                case TokenKind.Close:
                    depth--;
                    break;

                case TokenKind.Comma when depth == 0:
                    parts.Add(current);
                    current = new List<Token>();
                    continue;
            }

            current.Add(token);
        }

        if (!closed || parts.Any(x => x.Count == 0))
        {
            return null;
        }

        var columns = new List<StoredColumnDefinition>();
        var constraints = new List<StoredTableConstraint>();

        foreach (var part in parts)
        {
            var text = createTableSql[part[0].Start..part[^1].End];
            var first = part[0];

            if (first.Kind == TokenKind.Word && TableConstraintKeywords.Contains(first.Value))
            {
                // CONSTRAINT <name> <kind> ..., or the kind straight away
                var kind = first.Value.Equals("CONSTRAINT", StringComparison.OrdinalIgnoreCase)
                    ? part.Count > 2 ? part[2].Value : string.Empty
                    : first.Value;

                constraints.Add(new StoredTableConstraint(text, kind.ToUpperInvariant()));
                continue;
            }

            columns.Add(new StoredColumnDefinition(first.Value, text, referencedTables(part)));
        }

        return new StoredTableDefinition(columns, constraints);
    }

    /// <summary>
    ///     The tables an inline <c>REFERENCES</c> clause in a column definition points at. Each is a
    ///     single-column foreign key that the definition itself carries.
    /// </summary>
    private static IReadOnlyList<string> referencedTables(List<Token> part)
    {
        var tables = new List<string>();
        for (var i = 0; i < part.Count - 1; i++)
        {
            if (part[i].Kind == TokenKind.Word
                && part[i].Value.Equals("REFERENCES", StringComparison.OrdinalIgnoreCase)
                && part[i + 1].Kind is TokenKind.Word or TokenKind.Quoted)
            {
                tables.Add(part[i + 1].Value);
            }
        }

        return tables;
    }

    private enum TokenKind
    {
        Word,
        Quoted,
        Open,
        Close,
        Comma
    }

    /// <param name="Value">A word as written, or a quoted token's content with its delimiters and escapes removed</param>
    private readonly record struct Token(TokenKind Kind, string Value, int Start, int End);

    private static List<Token> tokenize(string sql)
    {
        var tokens = new List<Token>();
        var i = 0;

        while (i < sql.Length)
        {
            var c = sql[i];

            if (char.IsWhiteSpace(c))
            {
                i++;
                continue;
            }

            if (startsComment(sql, i))
            {
                i = skipComment(sql, i);
                continue;
            }

            switch (c)
            {
                case '(':
                    tokens.Add(new Token(TokenKind.Open, "(", i, i + 1));
                    i++;
                    continue;

                case ')':
                    tokens.Add(new Token(TokenKind.Close, ")", i, i + 1));
                    i++;
                    continue;

                case ',':
                    tokens.Add(new Token(TokenKind.Comma, ",", i, i + 1));
                    i++;
                    continue;

                case '\'' or '"' or '`' or '[':
                    var start = i;
                    var value = readQuoted(sql, ref i);
                    tokens.Add(new Token(TokenKind.Quoted, value, start, i));
                    continue;
            }

            var wordStart = i;
            while (i < sql.Length && !endsWord(sql, i))
            {
                i++;
            }

            tokens.Add(new Token(TokenKind.Word, sql[wordStart..i], wordStart, i));
        }

        return tokens;
    }

    private static bool startsComment(string sql, int i)
        => i + 1 < sql.Length && ((sql[i] == '-' && sql[i + 1] == '-') || (sql[i] == '/' && sql[i + 1] == '*'));

    private static int skipComment(string sql, int i)
    {
        if (sql[i] == '-')
        {
            var newline = sql.IndexOf('\n', i);
            return newline < 0 ? sql.Length : newline + 1;
        }

        var end = sql.IndexOf("*/", i + 2, StringComparison.Ordinal);
        return end < 0 ? sql.Length : end + 2;
    }

    private static bool endsWord(string sql, int i)
        => char.IsWhiteSpace(sql[i]) || sql[i] is '(' or ')' or ',' or '\'' or '"' or '`' or '['
                                     || startsComment(sql, i);

    /// <summary>
    ///     A string literal or delimited name. The closing character doubled is an escaped one,
    ///     except inside <c>[…]</c>, which has no escape. Leaves <paramref name="i" /> just past
    ///     the closing delimiter.
    /// </summary>
    private static string readQuoted(string sql, ref int i)
    {
        var close = sql[i] == '[' ? ']' : sql[i];
        var value = new StringBuilder();
        i++;

        while (i < sql.Length)
        {
            if (sql[i] != close)
            {
                value.Append(sql[i++]);
                continue;
            }

            if (close != ']' && i + 1 < sql.Length && sql[i + 1] == close)
            {
                value.Append(close);
                i += 2;
                continue;
            }

            i++;
            break;
        }

        return value.ToString();
    }
}

/// <param name="Name">The column name, undelimited</param>
/// <param name="Text">The whole definition, name included, exactly as stored</param>
/// <param name="ReferencedTables">The tables its inline <c>REFERENCES</c> clauses point at</param>
internal sealed record StoredColumnDefinition(string Name, string Text, IReadOnlyList<string> ReferencedTables);

/// <param name="Text">The whole constraint, exactly as stored</param>
/// <param name="Kind"><c>PRIMARY</c>, <c>UNIQUE</c>, <c>CHECK</c> or <c>FOREIGN</c></param>
internal sealed record StoredTableConstraint(string Text, string Kind);
