using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Postgresql.Tables.Partitioning;

public static class PartitionExtensions
{
    public static void WriteDefaultPartition(this TextWriter writer, DbObjectName identifier)
    {
        writer.WriteLine($"CREATE TABLE {identifier}_default PARTITION OF {identifier} DEFAULT;");
    }

    public static string FormatSqlValue<T>(this T value)
    {
        if (typeof(T).IsNumeric()) return value.ToString();
        return $"'{value.ToString()}'";
    }

}
