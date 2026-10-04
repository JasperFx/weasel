using Weasel.Core;

namespace Weasel.Firebird;

internal enum PsqlTokenKind
{
    Word,
    DelimitedIdentifier,
    Literal,
    Symbol
}

/// <summary>
///     One token of PSQL, by position in the text it was read from.
/// </summary>
internal readonly record struct PsqlToken(PsqlTokenKind Kind, int Start, int End)
{
    public string Text(string sql) => sql[Start..End];

    public bool IsWord(string sql, string word)
        => Kind == PsqlTokenKind.Word
           && End - Start == word.Length
           && string.Compare(sql, Start, word, 0, word.Length, StringComparison.OrdinalIgnoreCase) == 0;

    public bool IsSymbol(string sql, char symbol) => Kind == PsqlTokenKind.Symbol && sql[Start] == symbol;

    public bool IsIdentifier => Kind is PsqlTokenKind.Word or PsqlTokenKind.DelimitedIdentifier;
}

/// <summary>
///     Reads the header of a PSQL statement a token at a time, skipping whitespace and both comment
///     forms, so that a keyword inside a literal, a comment or a delimited identifier is never taken
///     for one of the header's own.
/// </summary>
internal static class PsqlLexer
{
    public static List<PsqlToken> Tokenize(string sql)
    {
        var tokens = new List<PsqlToken>();
        var position = 0;

        while (position < sql.Length)
        {
            var c = sql[position];

            if (char.IsWhiteSpace(c))
            {
                position++;
            }
            else if (c == '-' && next(sql, position) == '-')
            {
                var end = sql.IndexOf('\n', position);
                position = end < 0 ? sql.Length : end + 1;
            }
            else if (c == '/' && next(sql, position) == '*')
            {
                var end = sql.IndexOf("*/", position + 2, StringComparison.Ordinal);
                position = end < 0 ? sql.Length : end + 2;
            }
            else if (c is 'q' or 'Q' && next(sql, position) == '\'' && position + 2 < sql.Length)
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
                var stop = end < 0 ? sql.Length : end + 2;
                tokens.Add(new PsqlToken(PsqlTokenKind.Literal, position, stop));
                position = stop;
            }
            else if (c is '\'' or '"')
            {
                var stop = endOfQuoted(sql, position, c);
                tokens.Add(new PsqlToken(c == '"' ? PsqlTokenKind.DelimitedIdentifier : PsqlTokenKind.Literal,
                    position, stop));
                position = stop;
            }
            else if (isWordCharacter(c))
            {
                var start = position;
                while (position < sql.Length && isWordCharacter(sql[position]))
                {
                    position++;
                }

                tokens.Add(new PsqlToken(PsqlTokenKind.Word, start, position));
            }
            else
            {
                tokens.Add(new PsqlToken(PsqlTokenKind.Symbol, position, position + 1));
                position++;
            }
        }

        return tokens;
    }

    /// <summary>
    ///     The name as Firebird's catalog stores it: a delimited identifier exactly as written, anything
    ///     else folded to upper case.
    /// </summary>
    public static string CatalogIdentifier(string sql, PsqlToken token)
        => token.Kind == PsqlTokenKind.DelimitedIdentifier
            ? FirebirdIdentifierRules.Instance.Undelimit(token.Text(sql))
            : token.Text(sql).ToUpperInvariant();

    /// <summary>
    ///     The form two pieces of PSQL are compared in: whitespace and case outside literals do not
    ///     matter, a literal is compared character for character, and a trailing <c>;</c> is dropped.
    ///     Firebird keeps the source of a view, routine or trigger as it was written, so nothing else
    ///     needs absorbing.
    /// </summary>
    public static string NormalizeSource(string? source) => ViewSqlNormalizer.Normalize((source ?? string.Empty).Trim());

    private static char next(string sql, int position) => position + 1 < sql.Length ? sql[position + 1] : '\0';

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

    private static bool isWordCharacter(char c) => char.IsLetterOrDigit(c) || c == '_' || c == '$';
}
