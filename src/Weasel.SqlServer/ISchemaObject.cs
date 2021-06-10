using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;
using Weasel.Core;

namespace Weasel.SqlServer
{
    public interface ISchemaObject
    {
        DbObjectName Identifier { get; }
        void WriteCreateStatement(DdlRules rules, TextWriter writer);

        void WriteDropStatement(DdlRules rules, TextWriter writer);

        /// <summary>
        ///     Register the necessary queries to check the existing state of this schema
        ///     object in the database
        /// </summary>
        /// <param name="builder"></param>
        void ConfigureQueryCommand(CommandBuilder builder);

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
        protected SchemaObjectDelta(T expected, T actual)
        {
            if (expected == null)
            {
                throw new ArgumentNullException(nameof(expected));
            }

            Expected = expected;
            Actual = actual;

            Difference = compare(Expected, Actual);
        }

        public T Expected { get; }
        public T Actual { get; }

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

        protected abstract SchemaPatchDifference compare(T expected, T actual);
    }

    public class SchemaObjectDelta : ISchemaObjectDelta
    {
        public SchemaObjectDelta(ISchemaObject schemaObject, SchemaPatchDifference difference)
        {
            SchemaObject = schemaObject;
            Difference = difference;
        }

        public ISchemaObject SchemaObject { get; }
        public SchemaPatchDifference Difference { get; }

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
            throw new NotSupportedException();
        }
    }
}