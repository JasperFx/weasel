using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;

namespace Weasel.Postgresql
{
    public interface ISchemaObject
    {
        void Write(DdlRules rules, StringWriter writer);

        void WriteDropStatement(DdlRules rules, StringWriter writer);

        DbObjectName Identifier { get; }

        void ConfigureQueryCommand(CommandBuilder builder);

        Task<SchemaPatchDifference> CreatePatch(DbDataReader reader, SchemaPatch patch, AutoCreate autoCreate);

        IEnumerable<DbObjectName> AllNames();
    }
}
