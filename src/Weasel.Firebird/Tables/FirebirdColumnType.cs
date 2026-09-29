using System.Globalization;
using System.Text.RegularExpressions;

namespace Weasel.Firebird.Tables;

/// <summary>
///     A column type folded onto the spelling Weasel reads back out of Firebird's catalog, so that a
///     model and the catalog can be compared.
/// </summary>
/// <remarks>
///     <para>
///         Firebird stores a type, not the words it was declared with: <c>INT</c>, <c>CHARACTER
///         VARYING(10)</c>, <c>NATIONAL CHAR(10)</c> and <c>BINARY(16)</c> come back as
///         <c>INTEGER</c>, <c>VARCHAR(10)</c>, <c>CHAR(10) CHARACTER SET ISO8859_1</c> and
///         <c>CHAR(16) CHARACTER SET OCTETS</c>. Both sides are parsed into this, so a model written
///         with a synonym matches the column created from it (weasel#646's lesson, from MySQL).
///     </para>
///     <para>
///         The character set, the collation and a <c>NUMERIC</c>/<c>DECIMAL</c> precision are compared
///         only when the model states them (weasel#644). A model that says <c>VARCHAR(100)</c> has no
///         opinion about the database's default character set, and reporting drift against it would
///         produce a migration that can never converge.
///     </para>
///     <para>
///         <c>FLOAT(p)</c> is the one type whose storage depends on the server: Firebird 3 reads p as
///         decimal digits and stores a double from 8, Firebird 4 and later read it as binary digits and
///         switch at 25. Parsing a model therefore takes the version of the server it is compared with.
///     </para>
/// </remarks>
internal readonly record struct FirebirdColumnType(
    string Name,
    int? Length = null,
    int? Precision = null,
    int? Scale = null,
    string? SubType = null,
    bool WithTimeZone = false,
    string? CharacterSet = null,
    string? Collation = null)
{
    public const string Octets = "OCTETS";

    /// <summary>
    ///     The character set <c>NCHAR</c> and <c>NATIONAL CHARACTER</c> stand for.
    /// </summary>
    public const string NationalCharacterSet = "ISO8859_1";

    private static readonly Regex Whitespace = new(@"\s+", RegexOptions.Compiled);
    private static readonly Regex CollateClause = new(@"\sCOLLATE\s+(\S+)\s*$", RegexOptions.Compiled);
    private static readonly Regex CharacterSetClause = new(@"\sCHARACTER\s+SET\s+(\S+)", RegexOptions.Compiled);
    private static readonly Regex SegmentSizeClause = new(@"\sSEGMENT\s+SIZE\s+\d+", RegexOptions.Compiled);
    private static readonly Regex SubTypeClause = new(@"\sSUB_TYPE\s+(\S+)", RegexOptions.Compiled);

    private static readonly string[] IntegerRanks = ["SMALLINT", "INTEGER", "BIGINT", "INT128"];

    public bool IsCharacter => Name is "CHAR" or "VARCHAR";

    public bool IsExactNumeric => Name is "NUMERIC" or "DECIMAL";

    public bool IsInteger => Array.IndexOf(IntegerRanks, Name) >= 0;

    public override string ToString()
    {
        var text = Name switch
        {
            "CHAR" => $"CHAR({Length ?? 1})",
            "VARCHAR" => Length.HasValue ? $"VARCHAR({Length})" : "VARCHAR",
            "NUMERIC" or "DECIMAL" => Precision.HasValue ? $"{Name}({Precision},{Scale ?? 0})" : Name,
            "DECFLOAT" => $"DECFLOAT({Precision ?? 34})",
            "TIME" or "TIMESTAMP" => WithTimeZone ? $"{Name} WITH TIME ZONE" : Name,
            "BLOB" => $"BLOB SUB_TYPE {SubType ?? "BINARY"}",
            _ => Name
        };

        if (CharacterSet != null)
        {
            text += $" CHARACTER SET {CharacterSet}";
        }

        if (Collation != null)
        {
            text += $" COLLATE {Collation}";
        }

        return text;
    }

    /// <summary>
    ///     Does a column of type <paramref name="actual" /> -- read from the catalog -- satisfy this type,
    ///     the model's? Arguments the model leaves out are not compared.
    /// </summary>
    public bool IsSatisfiedBy(FirebirdColumnType actual)
    {
        if (Name != actual.Name)
        {
            return false;
        }

        switch (Name)
        {
            case "CHAR" when (Length ?? 1) != (actual.Length ?? 1):
            case "VARCHAR" when Length != actual.Length:
            case "NUMERIC" or "DECIMAL" when Precision.HasValue
                                           && (Precision != actual.Precision || (Scale ?? 0) != (actual.Scale ?? 0)):
            case "DECFLOAT" when (Precision ?? 34) != (actual.Precision ?? 34):
            case "TIME" or "TIMESTAMP" when WithTimeZone != actual.WithTimeZone:
            case "BLOB" when (SubType ?? "BINARY") != (actual.SubType ?? "BINARY"):
                return false;
        }

        if (CharacterSet != null && !string.Equals(CharacterSet, actual.CharacterSet, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        // A column on its character set's default collation is read back with no COLLATE, and that
        // collation is named after the character set: a model that says COLLATE UTF8 means exactly it.
        if (Collation != null
            && !string.Equals(Collation, actual.Collation ?? actual.CharacterSet, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return true;
    }

    /// <summary>
    ///     Can a column of type <paramref name="actual" /> be changed to this type with
    ///     <c>ALTER … TYPE</c>? Only the widening changes Firebird performs in place: a longer
    ///     <c>CHAR</c> or <c>VARCHAR</c> in the same character set, a wider integer, and a
    ///     <c>NUMERIC</c>/<c>DECIMAL</c> with more precision and the same scale. Firebird refuses the
    ///     rest -- a narrower column (336068816), a narrower integer (336068817), a character column to
    ///     a number (336068818) -- and cannot change a collation with <c>TYPE</c> at all.
    /// </summary>
    public bool CanAlterFrom(FirebirdColumnType actual)
    {
        if (IsCharacter)
        {
            return Name == actual.Name
                   && (Length ?? 1) >= (actual.Length ?? 1)
                   && (CharacterSet == null || string.Equals(CharacterSet, actual.CharacterSet, StringComparison.OrdinalIgnoreCase))
                   && (Collation == null || string.Equals(Collation, actual.Collation ?? actual.CharacterSet, StringComparison.OrdinalIgnoreCase));
        }

        if (IsInteger && actual.IsInteger)
        {
            return Array.IndexOf(IntegerRanks, Name) >= Array.IndexOf(IntegerRanks, actual.Name);
        }

        if (IsExactNumeric && Name == actual.Name)
        {
            return !Precision.HasValue
                   || (Precision >= actual.Precision && (Scale ?? 0) == (actual.Scale ?? 0));
        }

        if (Name == "DOUBLE PRECISION" && actual.Name == "FLOAT")
        {
            return true;
        }

        return IsSatisfiedBy(actual);
    }

    /// <summary>
    ///     Fold a declared type onto the catalog spelling. <paramref name="version" /> decides where
    ///     <c>FLOAT(p)</c> becomes a double; without one, Firebird 4's rule applies.
    /// </summary>
    public static FirebirdColumnType Parse(string type, FirebirdServerVersion? version = null)
    {
        var text = " " + Whitespace.Replace(type.Trim(), " ").ToUpperInvariant();

        string? collation = null;
        var collate = CollateClause.Match(text);
        if (collate.Success)
        {
            collation = collate.Groups[1].Value;
            text = text[..collate.Index];
        }

        string? characterSet = null;
        var charset = CharacterSetClause.Match(text);
        if (charset.Success)
        {
            characterSet = charset.Groups[1].Value;
            text = text.Remove(charset.Index, charset.Length);
        }

        text = text.Trim();

        if (text.StartsWith("BLOB", StringComparison.Ordinal))
        {
            return parseBlob(text) with { CharacterSet = characterSet, Collation = collation };
        }

        string head;
        string[] arguments = [];
        var open = text.IndexOf('(');
        var close = open < 0 ? -1 : text.IndexOf(')', open);
        if (open > 0 && close > open)
        {
            head = (text[..open] + text[(close + 1)..]).Trim();
            arguments = text[(open + 1)..close].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        }
        else
        {
            head = text;
        }

        int? first = arguments.Length > 0 ? integer(arguments[0]) : null;
        int? second = arguments.Length > 1 ? integer(arguments[1]) : null;

        switch (head)
        {
            case "INT" or "INTEGER":
                return new FirebirdColumnType("INTEGER");

            case "NUMERIC" or "DECIMAL" or "DEC":
                return new FirebirdColumnType(head == "DEC" ? "DECIMAL" : head, Precision: first,
                    Scale: first.HasValue ? second ?? 0 : null);

            case "REAL":
                return new FirebirdColumnType("FLOAT");

            case "FLOAT":
                var threshold = version?.DoublePrecisionFloatThreshold ?? 25;
                return new FirebirdColumnType(first >= threshold ? "DOUBLE PRECISION" : "FLOAT");

            case "DOUBLE" or "DOUBLE PRECISION" or "LONG FLOAT":
                return new FirebirdColumnType("DOUBLE PRECISION");

            case "DECFLOAT":
                return new FirebirdColumnType("DECFLOAT", Precision: first ?? 34);

            case "TIME" or "TIME WITHOUT TIME ZONE":
                return new FirebirdColumnType("TIME");

            case "TIMESTAMP" or "TIMESTAMP WITHOUT TIME ZONE":
                return new FirebirdColumnType("TIMESTAMP");

            case "TIME WITH TIME ZONE":
                return new FirebirdColumnType("TIME", WithTimeZone: true);

            case "TIMESTAMP WITH TIME ZONE":
                return new FirebirdColumnType("TIMESTAMP", WithTimeZone: true);

            case "CHAR" or "CHARACTER":
                return new FirebirdColumnType("CHAR", first ?? 1, CharacterSet: characterSet, Collation: collation);

            case "VARCHAR" or "CHAR VARYING" or "CHARACTER VARYING":
                return new FirebirdColumnType("VARCHAR", first, CharacterSet: characterSet, Collation: collation);

            case "NCHAR" or "NATIONAL CHAR" or "NATIONAL CHARACTER":
                return new FirebirdColumnType("CHAR", first ?? 1, CharacterSet: characterSet ?? NationalCharacterSet,
                    Collation: collation);

            case "NCHAR VARYING" or "NATIONAL CHAR VARYING" or "NATIONAL CHARACTER VARYING":
                return new FirebirdColumnType("VARCHAR", first, CharacterSet: characterSet ?? NationalCharacterSet,
                    Collation: collation);

            case "BINARY":
                return new FirebirdColumnType("CHAR", first ?? 1, CharacterSet: Octets);

            case "VARBINARY" or "BINARY VARYING":
                return new FirebirdColumnType("VARCHAR", first, CharacterSet: Octets);
        }

        return new FirebirdColumnType(arguments.Length == 0 ? head : $"{head}({string.Join(",", arguments)})",
            CharacterSet: characterSet, Collation: collation);
    }

    /// <summary>
    ///     <c>BLOB</c>, <c>BLOB SUB_TYPE TEXT</c>, <c>BLOB SUB_TYPE 1 SEGMENT SIZE 80</c>, <c>BLOB(80, 1)</c>.
    ///     The segment size is ignored: it is a client-side hint the catalog keeps but nothing compares.
    /// </summary>
    private static FirebirdColumnType parseBlob(string text)
    {
        var subType = "BINARY";

        var open = text.IndexOf('(');
        if (open > 0)
        {
            var close = text.IndexOf(')', open);
            var arguments = text[(open + 1)..(close < 0 ? text.Length : close)].Split(',', StringSplitOptions.TrimEntries);
            if (arguments.Length > 1)
            {
                subType = arguments[1];
            }
        }

        var clause = SubTypeClause.Match(" " + SegmentSizeClause.Replace(" " + text, ""));
        if (clause.Success)
        {
            subType = clause.Groups[1].Value;
        }

        subType = subType switch
        {
            "0" => "BINARY",
            "1" => "TEXT",
            _ => subType
        };

        return new FirebirdColumnType("BLOB", SubType: subType);
    }

    /// <summary>
    ///     The type the catalog describes, in the spelling a model writes -- the other half of
    ///     <see cref="Parse" />. Reads <c>RDB$FIELDS</c>: type, sub-type, precision, scale (negative, as
    ///     stored), length in characters, and the character set and collation names, with the collation
    ///     left out when it is the character set's default.
    /// </summary>
    public static FirebirdColumnType FromCatalog(
        int fieldType,
        int? subType,
        int? precision,
        int? scale,
        int? characterLength,
        string? characterSet,
        string? collation)
    {
        var sub = subType ?? 0;
        var digits = scale.HasValue ? -scale.Value : 0;

        switch (fieldType)
        {
            case 7 or 8 or 16 or 26 when sub is 1 or 2 || digits != 0:
                return new FirebirdColumnType(sub == 2 ? "DECIMAL" : "NUMERIC",
                    Precision: precision ?? fieldType switch { 7 => 4, 8 => 9, 16 => 18, _ => 38 },
                    Scale: digits);
            case 7:
                return new FirebirdColumnType("SMALLINT");
            case 8:
                return new FirebirdColumnType("INTEGER");
            case 16:
                return new FirebirdColumnType("BIGINT");
            case 26:
                return new FirebirdColumnType("INT128");
            case 10:
                return new FirebirdColumnType("FLOAT");
            case 27:
                return new FirebirdColumnType("DOUBLE PRECISION");
            case 24:
                return new FirebirdColumnType("DECFLOAT", Precision: 16);
            case 25:
                return new FirebirdColumnType("DECFLOAT", Precision: 34);
            case 23:
                return new FirebirdColumnType("BOOLEAN");
            case 12:
                return new FirebirdColumnType("DATE");
            case 13:
                return new FirebirdColumnType("TIME");
            case 35:
                return new FirebirdColumnType("TIMESTAMP");
            case 28:
                return new FirebirdColumnType("TIME", WithTimeZone: true);
            case 29:
                return new FirebirdColumnType("TIMESTAMP", WithTimeZone: true);
            case 14:
                // Sub-type 1 is BINARY(n) on Firebird 4 and later, which is CHAR(n) CHARACTER SET OCTETS
                // under another name.
                return new FirebirdColumnType("CHAR", characterLength,
                    CharacterSet: sub == 1 ? Octets : characterSet, Collation: sub == 1 ? null : collation);
            case 37:
                return new FirebirdColumnType("VARCHAR", characterLength,
                    CharacterSet: sub == 1 ? Octets : characterSet, Collation: sub == 1 ? null : collation);
            case 261:
                return sub == 1
                    ? new FirebirdColumnType("BLOB", SubType: "TEXT", CharacterSet: characterSet, Collation: collation)
                    : new FirebirdColumnType("BLOB", SubType: sub == 0 ? "BINARY" : sub.ToString(CultureInfo.InvariantCulture));
        }

        return new FirebirdColumnType($"UNKNOWN({fieldType.ToString(CultureInfo.InvariantCulture)})");
    }

    private static int? integer(string text)
        => int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : null;
}
