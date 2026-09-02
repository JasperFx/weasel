using System.Globalization;

namespace Weasel.Core;

/// <summary>
///     Reads the declared length out of a character or binary column type so that column comparison can
///     tell <c>varchar(255)</c> from <c>varchar(1000)</c>.
/// </summary>
/// <remarks>
///     Every provider's <c>TableColumn</c> comparison strips the parenthesised part of a type before
///     comparing, because for most types it is not a length that a model is expected to reproduce: MySQL
///     reports an <c>INT</c> display width the catalog invented, <c>DECIMAL</c> carries a precision and a
///     scale, <c>DATETIME</c> a fractional-seconds precision. Comparing those wholesale produces drift on
///     every schema check for tables nobody touched.
///     <para>
///     Character and binary lengths are the exception. They are declared by the model, reported faithfully
///     by every catalog, and the difference is load-bearing -- a column narrower than the value being
///     written fails the insert. Before this, widening a <c>varchar</c> in a model was invisible to the
///     differ, so an existing database silently kept the old width forever and only a hand-written ALTER
///     fixed it. See JasperFx/wolverine#4246, where a MySQL <c>varchar(255)</c> that should have been
///     wider failed node-record inserts with "Data too long for column".
///     </para>
/// </remarks>
public static class CharacterColumnLength
{
    /// <summary>
    ///     Sentinel for the unbounded forms -- SQL Server's <c>varchar(max)</c>. Compares equal only to
    ///     another unbounded declaration, and greater than any explicit length.
    /// </summary>
    public const int Unbounded = int.MaxValue;

    // Deliberately only the types whose single parenthesised argument is a character or byte length.
    // Anything else keeps the existing size-insensitive comparison.
    private static readonly HashSet<string> LengthBearingTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "CHAR",
        "VARCHAR",
        "NCHAR",
        "NVARCHAR",
        "CHARACTER",
        "VARCHAR2",
        "NVARCHAR2",
        "BINARY",
        "VARBINARY",
        "RAW"
    };

    /// <summary>
    ///     True when a length declared on this type names a character or byte count.
    /// </summary>
    public static bool IsLengthBearing(string? rawType)
        => rawType != null && LengthBearingTypes.Contains(rawType.Trim());

    /// <summary>
    ///     The declared length of <paramref name="type" />, or null when the type carries none or is not a
    ///     type whose length is comparable. A null on either side of a comparison means "cannot tell", and
    ///     the caller should fall back to comparing the bare type -- never report drift on a guess.
    /// </summary>
    public static int? TryParse(string? type)
    {
        if (type == null) return null;

        var open = type.IndexOf('(');
        if (open < 0) return null;

        var close = type.IndexOf(')', open);
        if (close < 0) return null;

        if (!IsLengthBearing(type[..open])) return null;

        var inner = type[(open + 1)..close].Trim();

        // Oracle spells the length semantics out: VARCHAR2(100 CHAR) and VARCHAR2(100 BYTE) are both
        // 100 as far as a model is concerned, and the catalog reports the byte count either way.
        var space = inner.IndexOf(' ');
        if (space > 0)
        {
            inner = inner[..space];
        }

        if (inner.Equals("max", StringComparison.OrdinalIgnoreCase)) return Unbounded;

        return int.TryParse(inner, NumberStyles.Integer, CultureInfo.InvariantCulture, out var length)
            ? length
            : null;
    }

    /// <summary>
    ///     True when the two type declarations disagree about a character length that both of them state.
    ///     False whenever either side declares none, so a model that omits a length keeps comparing the way
    ///     it always has.
    /// </summary>
    public static bool Differ(string? expected, string? actual)
    {
        var expectedLength = TryParse(expected);
        if (expectedLength == null) return false;

        var actualLength = TryParse(actual);
        if (actualLength == null) return false;

        return expectedLength != actualLength;
    }
}
