using System.Globalization;

namespace Weasel.MySql.Tables;

/// <summary>
///     A column type folded onto the spelling MySQL reports for it in
///     <c>information_schema.COLUMNS.COLUMN_TYPE</c>, so that a model and the catalog can be compared.
/// </summary>
/// <remarks>
///     <para>
///         MySQL rewrites a type when it stores the column: <c>BOOLEAN</c> is reported as
///         <c>tinyint(1)</c>, <c>INTEGER</c> as <c>int</c>, <c>NUMERIC(13,4)</c> as <c>decimal(13,4)</c>,
///         <c>DOUBLE PRECISION</c> as <c>double</c>. Comparing the declared spelling verbatim reported
///         drift on every check for any model written with a synonym, and every migration re-issued a
///         <c>MODIFY COLUMN</c> that changed nothing.
///     </para>
///     <para>
///         Only what <c>COLUMN_TYPE</c> can report takes part: the type name, its parenthesised
///         arguments, and the numeric attributes <c>UNSIGNED</c> and <c>ZEROFILL</c>. Anything else a
///         model writes after the type -- a character set, a collation, a column attribute -- is never
///         reported there, so comparing it could only produce drift a migration cannot resolve. That
///         matches what the comparison already did for a type with arguments, which discarded
///         everything after the closing parenthesis.
///     </para>
///     <para>
///         <c>REAL</c> folds to <c>DOUBLE</c>, which is what it means unless the server runs with the
///         <c>REAL_AS_FLOAT</c> SQL mode.
///     </para>
/// </remarks>
internal readonly record struct MySqlColumnType(string Name, string? Arguments, bool Unsigned, bool Zerofill)
{
    private static readonly char[] Whitespace = [' ', '\t', '\r', '\n'];

    // Longest spellings first, so NATIONAL CHARACTER VARYING is not read as NATIONAL CHARACTER. All of
    // these are matched before the single words, so NCHAR VARYING is not read as NCHAR.
    private static readonly (string[] Words, string Name)[] MultiWordSynonyms =
    [
        (["NATIONAL", "CHARACTER", "VARYING"], "VARCHAR"),
        (["NATIONAL", "CHAR", "VARYING"], "VARCHAR"),
        (["NATIONAL", "CHARACTER"], "CHAR"),
        (["NATIONAL", "CHAR"], "CHAR"),
        (["NATIONAL", "VARCHAR"], "VARCHAR"),
        (["NCHAR", "VARYING"], "VARCHAR"),
        (["NCHAR", "VARCHAR"], "VARCHAR"),
        (["CHARACTER", "VARYING"], "VARCHAR"),
        (["CHAR", "VARYING"], "VARCHAR"),
        (["DOUBLE", "PRECISION"], "DOUBLE"),
        (["LONG", "VARBINARY"], "MEDIUMBLOB"),
        (["LONG", "VARCHAR"], "MEDIUMTEXT")
    ];

    private static readonly Dictionary<string, string> SingleWordSynonyms = new(StringComparer.Ordinal)
    {
        ["INTEGER"] = "INT",
        ["INT1"] = "TINYINT",
        ["INT2"] = "SMALLINT",
        ["INT3"] = "MEDIUMINT",
        ["MIDDLEINT"] = "MEDIUMINT",
        ["INT4"] = "INT",
        ["INT8"] = "BIGINT",
        ["NUMERIC"] = "DECIMAL",
        ["DEC"] = "DECIMAL",
        ["FIXED"] = "DECIMAL",
        ["REAL"] = "DOUBLE",
        ["FLOAT4"] = "FLOAT",
        ["FLOAT8"] = "DOUBLE",
        ["CHARACTER"] = "CHAR",
        ["NCHAR"] = "CHAR",
        ["NVARCHAR"] = "VARCHAR",
        ["VARCHARACTER"] = "VARCHAR",
        ["LONG"] = "MEDIUMTEXT"
    };

    /// <summary>
    ///     The type without its parenthesised arguments -- a display width, a precision and scale, a
    ///     character length -- which is what the column comparison matches on. A character length is
    ///     compared separately, through <see cref="Weasel.Core.CharacterColumnLength" />.
    /// </summary>
    public string WithoutArguments => Name + (Unsigned ? " UNSIGNED" : "") + (Zerofill ? " ZEROFILL" : "");

    public override string ToString()
        => Name + (Arguments == null ? "" : $"({Arguments})") + (Unsigned ? " UNSIGNED" : "")
           + (Zerofill ? " ZEROFILL" : "");

    public static MySqlColumnType Parse(string type)
    {
        var text = type.Trim().ToUpperInvariant();

        string head;
        string? arguments = null;
        var tail = "";

        var open = text.IndexOf('(');
        var close = open < 0 ? -1 : text.IndexOf(')', open);
        if (open > 0 && close > open)
        {
            head = text[..open];
            arguments = text[(open + 1)..close].Trim();
            tail = text[(close + 1)..];
        }
        else
        {
            head = text;
        }

        var words = head.Split(Whitespace, StringSplitOptions.RemoveEmptyEntries);
        if (words.Length == 0)
        {
            return new MySqlColumnType(text, arguments, false, false);
        }

        var (name, consumed) = readName(words);
        var attributes = words.Skip(consumed)
            .Concat(tail.Split(Whitespace, StringSplitOptions.RemoveEmptyEntries))
            .ToArray();

        var zerofill = attributes.Contains("ZEROFILL");

        // ZEROFILL implies UNSIGNED, and MySQL reports both.
        var unsigned = zerofill || attributes.Contains("UNSIGNED");

        switch (name)
        {
            case "SERIAL":
                // An alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE. Only the type folds here:
                // the UNIQUE index it implies is not in the model, so a SERIAL column that is not the
                // primary key still reports that index as an extra.
                return new MySqlColumnType("BIGINT", null, true, zerofill);

            case "BOOL" or "BOOLEAN":
                return new MySqlColumnType("TINYINT", "1", unsigned, zerofill);

            case "FLOAT" when arguments != null && !arguments.Contains(','):
                // FLOAT(p) picks the storage from the precision alone: 0-24 is FLOAT, 25-53 is DOUBLE,
                // and the catalog reports neither with arguments.
                return int.TryParse(arguments, NumberStyles.Integer, CultureInfo.InvariantCulture, out var precision)
                       && precision > 24
                    ? new MySqlColumnType("DOUBLE", null, unsigned, zerofill)
                    : new MySqlColumnType("FLOAT", null, unsigned, zerofill);
        }

        if (SingleWordSynonyms.TryGetValue(name, out var canonical))
        {
            name = canonical;
        }

        return new MySqlColumnType(name, arguments, unsigned, zerofill);
    }

    private static (string Name, int Consumed) readName(string[] words)
    {
        foreach (var (synonym, name) in MultiWordSynonyms)
        {
            if (words.Length >= synonym.Length && words.Take(synonym.Length).SequenceEqual(synonym))
            {
                return (name, synonym.Length);
            }
        }

        return (words[0], 1);
    }
}
