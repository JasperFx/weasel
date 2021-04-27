using System.IO;

namespace Weasel.Postgresql.Tables
{
    internal static class StringWriterExtensions
    {
        public static void WriteCascadeAction(this StringWriter writer, string prefix, CascadeAction action)
        {
            switch (action)
            {
                case CascadeAction.Cascade:
                    writer.WriteLine($"{prefix} CASCADE");
                    break;
                case CascadeAction.Restrict:
                    writer.WriteLine($"{prefix} RESTRICT");
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
    }
}