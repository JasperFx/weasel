namespace Weasel.MySql.Tables;

internal static class StringWriterExtensions
{
    public static void WriteStatement(this StringWriter writer, string statement)
    {
        writer.WriteLine(statement);
        if (!statement.TrimEnd().EndsWith(";"))
        {
            writer.WriteLine(";");
        }
    }
}
