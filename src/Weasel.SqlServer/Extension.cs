using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using Weasel.Core;

namespace Weasel.SqlServer
{
    /// <summary>
    ///     Used to register Postgresql extensions
    /// </summary>
    public class Extension : ISchemaObject
    {
        public Extension(string extensionName)
        {
            ExtensionName = extensionName.Trim().ToLower();
        }

        public string ExtensionName { get; }

        public void WriteCreateStatement(DdlRules rules, TextWriter writer)
        {
            writer.WriteLine($"CREATE EXTENSION IF NOT EXISTS {ExtensionName};");
        }

        public void WriteDropStatement(DdlRules rules, TextWriter writer)
        {
            writer.WriteLine($"DROP EXTENSION IF EXISTS {ExtensionName} CASCADE;");
        }

        public DbObjectName Identifier => new("public", ExtensionName);

        public void ConfigureQueryCommand(CommandBuilder builder)
        {
            builder.Append("select extname from pg_extension where extname = ");
            builder.AppendParameter(ExtensionName);
            builder.Append(";");
        }

        public async Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader)
        {
            var exists = await reader.ReadAsync();

            return new SchemaObjectDelta(this, exists ? SchemaPatchDifference.None : SchemaPatchDifference.Create);
        }

        public IEnumerable<DbObjectName> AllNames()
        {
            yield return Identifier;
        }
    }
}