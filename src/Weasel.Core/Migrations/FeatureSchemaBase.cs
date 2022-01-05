using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Weasel.Core.Migrations
{
    /// <summary>
    /// Base class for easier creation of custom IFeatureSchema objects
    /// </summary>
    public abstract class FeatureSchemaBase: IFeatureSchema
    {
        public string Identifier { get; }
        public Migrator Migrator { get; }

        protected FeatureSchemaBase(string identifier, Migrator migrator)
        {
            Identifier = identifier;
            Migrator = migrator;
        }

        public virtual IEnumerable<Type> DependentTypes()
        {
            return Type.EmptyTypes;
        }

        protected abstract IEnumerable<ISchemaObject> schemaObjects();

        public ISchemaObject[] Objects => schemaObjects().ToArray();

        public virtual Type StorageType => GetType();

        public virtual void WritePermissions(Migrator rules, TextWriter writer)
        {
            // Nothing
        }
    }
}
