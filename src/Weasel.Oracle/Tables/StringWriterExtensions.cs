namespace Weasel.Oracle.Tables;

internal static class StringWriterExtensions
{
    public static void WriteCascadeAction(this TextWriter writer, string prefix, CascadeAction action)
    {
        switch (action)
        {
            case CascadeAction.Cascade:
                writer.WriteLine($"{prefix} CASCADE");
                break;
            case CascadeAction.NoAction:
                return;
            case CascadeAction.SetDefault:
                writer.WriteLine($"{prefix} SET DEFAULT");
                break;
            case CascadeAction.SetNull:
                writer.WriteLine($"{prefix} SET NULL");
                break;
        }
    }

    public static void WriteDropIndex(this TextWriter writer, Table table, IndexDefinition index)
    {
        writer.WriteLine($"DROP INDEX {index.Name}");
    }
}
