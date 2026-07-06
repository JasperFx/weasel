using System.Globalization;
using JasperFx.Core.Reflection;
using Weasel.Core;
using Weasel.Postgresql;

namespace Weasel.Postgresql.Tables.Partitioning;

public static class PartitionExtensions
{
    /// <summary>
    /// Write the SQL for the default partition for the given table name. Uses "_default" as the suffix
    /// </summary>
    /// <param name="writer"></param>
    /// <param name="identifier"></param>
    public static void WriteDefaultPartition(this TextWriter writer, DbObjectName identifier)
    {
        var partitionName = PostgresqlObjectName.From(
            new DbObjectName(identifier.Schema, identifier.Name + "_default"));
        var parentName = PostgresqlObjectName.From(identifier);
        // IF NOT EXISTS: keep partition creation idempotent under concurrent schema application.
        writer.WriteLine($"CREATE TABLE IF NOT EXISTS {partitionName} PARTITION OF {parentName} DEFAULT;");
    }

    /// <summary>
    /// Write the literal value of SQL for a given value. Places single quotes around strings
    /// </summary>
    /// <param name="value"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public static string FormatSqlValue<T>(this T value)
    {
        // DateTimeOffset/DateTime have to be rendered with an invariant, canonical format so that the
        // generated partition bound DDL is a) valid PostgreSQL and b) deterministic regardless of the
        // current thread culture (the default ToString() is culture sensitive). The round-trip comparison
        // against what PostgreSQL echoes back is handled separately via NormalizePartitionValue.
        if (value is DateTimeOffset dto)
        {
            return $"'{dto.ToString("yyyy-MM-dd HH:mm:ss.FFFFFFzzz", CultureInfo.InvariantCulture)}'";
        }

        if (value is DateTime dt)
        {
            return $"'{dt.ToString("yyyy-MM-dd HH:mm:ss.FFFFFF", CultureInfo.InvariantCulture)}'";
        }

        if (typeof(T).IsNumeric()) return value.ToString();

        if (value is bool b) return b.ToString().ToLowerInvariant();

        if (value is string v && v.StartsWith("'") && v.EndsWith("'")) return v;

        return $"'{value.ToString()}'";
    }

    /// <summary>
    /// Normalize a partition bound literal (either a declared value produced by <see cref="FormatSqlValue{T}"/>
    /// or a value echoed back by PostgreSQL via <c>pg_get_expr(relpartbound)</c>) into a stable, comparable
    /// form. PostgreSQL single-quotes every bound literal on read-back (so a declared <c>20</c> comes back as
    /// <c>'20'</c>) and renders <c>timestamptz</c> bounds in the session time zone (so <c>'2026-01-01 00:00:00+00'</c>
    /// can come back as <c>'2025-12-31 18:00:00-06'</c>). Both would otherwise produce spurious migration
    /// rebuilds when compared as raw strings.
    /// </summary>
    internal static string NormalizePartitionValue(this string? raw)
    {
        if (raw is null) return string.Empty;

        var value = raw.Trim();
        if (value.Length >= 2 && value[0] == '\'' && value[^1] == '\'')
        {
            value = value.Substring(1, value.Length - 2);
        }

        if (LooksLikeTimestamp(value)
            && DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var moment))
        {
            return moment.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz", CultureInfo.InvariantCulture);
        }

        return value;
    }

    private static bool LooksLikeTimestamp(string value)
    {
        // Cheap guard so plain integers ('20') and text values ('role-a') are never misparsed as dates:
        // a timestamp/date literal starts with a four digit year followed by a '-' separator.
        return value.Length >= 8
            && value.IndexOf('-') >= 4
            && char.IsDigit(value[0]) && char.IsDigit(value[1])
            && char.IsDigit(value[2]) && char.IsDigit(value[3]);
    }

    /// <summary>
    /// Picks off the suffix name for a partition name based on a table name
    /// </summary>
    /// <param name="identifier"></param>
    /// <param name="tableName"></param>
    /// <returns></returns>
    public static string GetSuffixName(this DbObjectName identifier, string tableName)
    {
        var suffix = tableName;
        if (tableName.StartsWith(identifier.Name))
        {
            suffix = suffix.Substring(identifier.Name.Length);
        }

        return suffix.TrimStart('_');
    }

    internal static string GetStringWithinParantheses(this string raw)
    {
        var start = raw.IndexOf('(');
        var end = raw.IndexOf(')');
        return raw.Substring(start + 1, end - start - 1);
    }

    internal static ReadOnlySpan<char> GetSpanWithinParentheses(this string raw)
    {
        var start = raw.IndexOf('(');
        var end = raw.IndexOf(')');
        return raw.AsSpan(start + 1, end - start - 1);
    }

}
