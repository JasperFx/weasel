using System.Collections.Generic;
using System.IO;

namespace Weasel.SqlServer
{
    internal class SchemaGenerator
    {
        private const string BeginScript = @"DO $$
BEGIN";

        private const string EndScript = @"END
$$;
";


        public static void WriteSql(IEnumerable<string> schemaNames, TextWriter writer)
        {
            //writer.Write(BeginScript);

            foreach (var schemaName in schemaNames) WriteSql(schemaName, writer);

            //writer.WriteLine(EndScript);
            writer.WriteLine();
        }

        private static void WriteSql(string databaseSchemaName, TextWriter writer)
        {
            writer.WriteLine(SchemaMigration.CreateSchemaStatementFor(databaseSchemaName));
        }
    }
}