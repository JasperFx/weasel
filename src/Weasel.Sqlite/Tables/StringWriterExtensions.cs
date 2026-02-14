using Weasel.Core;

namespace Weasel.Sqlite.Tables;

internal static class StringWriterExtensions
{
    public static void WriteCascadeAction(this TextWriter writer, string prefix, CascadeAction action)
    {
        switch (action)
        {
            case CascadeAction.Cascade:
                writer.Write($" {prefix} CASCADE");
                break;
            case CascadeAction.Restrict:
                writer.Write($" {prefix} RESTRICT");
                break;
            case CascadeAction.NoAction:
                writer.Write($" {prefix} NO ACTION");
                break;
            case CascadeAction.SetDefault:
                writer.Write($" {prefix} SET DEFAULT");
                break;
            case CascadeAction.SetNull:
                writer.Write($" {prefix} SET NULL");
                break;
        }
    }

    public static void WriteDropIndex(this TextWriter writer, Table table, IndexDefinition index)
    {
        // SQLite: DROP INDEX [IF EXISTS] index_name
        writer.WriteLine($"DROP INDEX IF EXISTS {index.Name};");
    }
}
