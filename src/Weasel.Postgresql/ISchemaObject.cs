using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;

namespace Weasel.Postgresql
{
    public interface ISchemaObject
    {
        void WriteCreateStatement(DdlRules rules, StringWriter writer);

        void WriteDropStatement(DdlRules rules, StringWriter writer);

        DbObjectName Identifier { get; }

        /// <summary>
        /// Register the necessary queries to check the existing state of this schema
        /// object in the database
        /// </summary>
        /// <param name="builder"></param>
        void ConfigureQueryCommand(CommandBuilder builder);

        [Obsolete("Let's move this to CreateDelta")]
        Task<SchemaPatchDifference> CreatePatch(DbDataReader reader, SchemaPatch patch, AutoCreate autoCreate);

        Task<ISchemaObjectDelta> CreateDelta(DbDataReader reader);
        
        IEnumerable<DbObjectName> AllNames();
    }


    public interface ISchemaObjectDelta
    {
        ISchemaObject SchemaObject { get; }
        SchemaPatchDifference Difference { get; }

        void WriteUpdate(DdlRules rules, StringWriter writer);
        void WriteRollback(DdlRules rules, StringWriter writer);
    }

    public class SchemaObjectDelta : ISchemaObjectDelta
    {
        public ISchemaObject SchemaObject { get; }
        public SchemaPatchDifference Difference { get; }

        public SchemaObjectDelta(ISchemaObject schemaObject, SchemaPatchDifference difference)
        {
            SchemaObject = schemaObject;
            Difference = difference;
        }

        public void WriteUpdate(DdlRules rules, StringWriter writer)
        {
            SchemaObject.WriteDropStatement(rules, writer);
            SchemaObject.WriteCreateStatement(rules, writer);
        }

        public void WriteRollback(DdlRules rules, StringWriter writer)
        {
            
        }
    }
    
    
}
