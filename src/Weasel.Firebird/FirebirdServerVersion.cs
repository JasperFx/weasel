using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;
using FirebirdSql.Data.FirebirdClient;

namespace Weasel.Firebird;

/// <summary>
///     The version of the Firebird server a connection is attached to, and the handful of behaviours
///     that differ between 3, 4 and 5.
/// </summary>
/// <remarks>
///     DDL is written before any connection is open and has to run on all three versions, so nothing
///     that renders a statement asks for this. It is read where the catalog is: to decide whether an
///     introspection query can name a column only Firebird 5 has, and how to read back a type whose
///     storage changed between versions.
/// </remarks>
public readonly record struct FirebirdServerVersion(int Major, int Minor, int Patch)
{
    private static readonly Regex VersionPattern = new(@"(\d+)\.(\d+)(?:\.(\d+))?", RegexOptions.Compiled);

    /// <summary>
    ///     Partial indexes (<c>CREATE INDEX … WHERE</c>) and <c>RDB$INDICES.RDB$CONDITION_SOURCE</c>
    ///     arrived in Firebird 5.
    /// </summary>
    public bool SupportsPartialIndexes => Major >= 5;

    /// <summary>
    ///     <c>TIME/TIMESTAMP WITH TIME ZONE</c>, <c>INT128</c>, <c>DECFLOAT</c> and
    ///     <c>BINARY/VARBINARY</c> arrived in Firebird 4.
    /// </summary>
    public bool SupportsFirebird4Types => Major >= 4;

    /// <summary>
    ///     The longest identifier the server accepts: 31 bytes on Firebird 3, 63 characters from 4 on.
    /// </summary>
    public int MaxIdentifierLength => Major >= 4 ? 63 : 31;

    /// <summary>
    ///     The smallest <c>FLOAT(p)</c> precision the server stores as <c>DOUBLE PRECISION</c>. Firebird 3
    ///     reads p as decimal digits, so 8 already needs a double; Firebird 4 reads it as binary digits,
    ///     as the SQL standard does, and switches at 25.
    /// </summary>
    public int DoublePrecisionFloatThreshold => Major >= 4 ? 25 : 8;

    /// <summary>
    ///     Firebird 3 sets a restarted or newly created sequence so that the next value is one increment
    ///     past the value given; Firebird 4 and later make the next value the one given.
    /// </summary>
    public bool NextValueFollowsStartValue => Major < 4;

    public override string ToString() => $"{Major}.{Minor}.{Patch}";

    /// <summary>
    ///     Read a version out of <see cref="FbConnection.ServerVersion" /> (<c>LI-V5.0.4.1812 Firebird
    ///     5.0/tcp …</c>) or <c>rdb$get_context('SYSTEM', 'ENGINE_VERSION')</c> (<c>5.0.4</c>).
    /// </summary>
    public static FirebirdServerVersion? TryParse(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return null;
        }

        var match = VersionPattern.Match(text);
        if (!match.Success)
        {
            return null;
        }

        return new FirebirdServerVersion(
            int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture),
            int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture),
            match.Groups[3].Success ? int.Parse(match.Groups[3].Value, CultureInfo.InvariantCulture) : 0);
    }

    /// <summary>
    ///     The version of the server <paramref name="connection" /> is attached to, or null when it is
    ///     not an open Firebird connection.
    /// </summary>
    public static FirebirdServerVersion? Of(DbConnection? connection)
    {
        if (connection is not FbConnection { State: ConnectionState.Open } firebird)
        {
            return null;
        }

        return TryParse(firebird.ServerVersion);
    }
}
