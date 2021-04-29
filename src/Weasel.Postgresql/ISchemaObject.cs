using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;

namespace Weasel.Postgresql
{
    public interface ISchemaObject
    {
        void WriteCreateStatement(DdlRules rules, TextWriter writer);

        void WriteDropStatement(DdlRules rules, TextWriter writer);

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

        void WriteUpdate(DdlRules rules, TextWriter writer);
        void WriteRollback(DdlRules rules, TextWriter writer);
        void WriteRestorationOfPreviousState(DdlRules rules, TextWriter writer);
    }

    public abstract class SchemaObjectDelta<T> : ISchemaObjectDelta where T : ISchemaObject
    {
        public T Expected { get; }
        public T Actual { get; }

        protected SchemaObjectDelta(T expected, T actual)
        {
            Expected = expected;
            Actual = actual;

            Difference = compare(Expected, Actual);
        }

        protected abstract SchemaPatchDifference compare(T expected, T actual);

        public ISchemaObject SchemaObject => Expected;

        public SchemaPatchDifference Difference { get; }
        public abstract void WriteUpdate(DdlRules rules, TextWriter writer);

        public virtual void WriteRollback(DdlRules rules, TextWriter writer)
        {
            Expected.WriteDropStatement(rules, writer);
            Actual.WriteCreateStatement(rules, writer);
        }

        public void WriteRestorationOfPreviousState(DdlRules rules, TextWriter writer)
        {
            Actual.WriteCreateStatement(rules, writer);
        }
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

        public void WriteUpdate(DdlRules rules, TextWriter writer)
        {
            SchemaObject.WriteDropStatement(rules, writer);
            SchemaObject.WriteCreateStatement(rules, writer);
        }

        public void WriteRollback(DdlRules rules, TextWriter writer)
        {
            
        }

        public void WriteRestorationOfPreviousState(DdlRules rules, TextWriter writer)
        {
            throw new NotImplementedException();
        }
    }
    
    
}
