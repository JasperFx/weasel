using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace Weasel.Postgresql.Migrations
{
    public class DatabaseSpecification
    {
        public string? Encoding { get; set; }
        public string? Owner { get; set; }
        public int? ConnectionLimit { get; set; }
        public string? LcCollate { get; set; }
        public string? LcType { get; set; }
        public string? TableSpace { get; set; }

        public string ToCreateDatabaseStatement(string databaseName)
        {
            var withs = new List<string>();
            if (Encoding != null) withs.Add($" ENCODING = '{Encoding}'");
            if (Owner != null) withs.Add($" OWNER = '{Owner}'");
            if (ConnectionLimit.HasValue) withs.Add($" CONNECTION LIMIT = {ConnectionLimit}");
            if (LcCollate != null) withs.Add($" LC_COLLATE = '{LcCollate}'");
            if (LcType != null) withs.Add($" LC_CTYPE = '{LcType}'");
            if (TableSpace != null) withs.Add($" TABLESPACE  = {TableSpace}");

            var builder = new StringBuilder();
            builder.Append($"CREATE DATABASE \"{databaseName}\"");
            if (withs.Any())
            {
                builder.Append(" WITH");
            }

            foreach (var @with in withs)
            {
                builder.Append(@with);
            }

            return builder.ToString();

        }

        public Task BuildDatabase(NpgsqlConnection conn, string databaseName)
        {
            return conn.CreateCommand(ToCreateDatabaseStatement(databaseName))
                .ExecuteNonQueryAsync();
        }
    }
}
