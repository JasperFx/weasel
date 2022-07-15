using System.IO;
using System.Linq;
using Baseline;
using Weasel.Core;
using Weasel.Postgresql.Tables;

namespace Weasel.Postgresql.Functions
{
    /// <summary>
    /// Recipe for creating a simple upsert function based on a table structure
    /// </summary>
    public class UpsertFunction : Function
    {
        private readonly DbObjectName _identifier;
        private readonly Table _table;
        private readonly string[] _columns;

        public UpsertFunction(DbObjectName identifier, Table table, params string[] columns) : base(identifier)
        {
            _identifier = identifier;
            _table = table;
            _columns = columns;
        }

        public override void WriteCreateStatement(Migrator migrator, TextWriter writer)
        {
            var pkColumns = _table.PrimaryKeyColumns.Select(x => _table.ColumnFor(x)).ToArray();

            var columns = _columns.Select(x => _table.ColumnFor(x)).ToArray();

            var inserts = _table.PrimaryKeyColumns.Concat(_columns).Join(", ");
            var argList = pkColumns.Concat(columns).Select(x => x.ToFunctionArgumentDeclaration()).Join(", ");
            var valueList = pkColumns.Concat(columns).Select(x => x.ToArgumentName()).Join(", ");
            var updates = columns.Select(x => x.ToFunctionUpdate()).Join(", ");

            writer.WriteLine($@"
CREATE OR REPLACE FUNCTION {Identifier.QualifiedName}({argList}) RETURNS void LANGUAGE plpgsql
AS $function$
DECLARE
  final_version uuid;
BEGIN
INSERT INTO {_table.Identifier.QualifiedName} ({inserts}) VALUES ({valueList})
  ON CONFLICT ON CONSTRAINT {_table.PrimaryKeyName}
  DO UPDATE SET {updates};
END;
$function$;
");
        }
    }
}
