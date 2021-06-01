using System;
using System.Collections.Generic;
using System.Linq;
using Baseline;
#nullable enable

namespace Weasel.Postgresql
{
    public class SchemaMigrationException : Exception
    {
        public SchemaMigrationException(string? message) : base(message)
        {
        }

        public SchemaMigrationException(string? message, Exception? innerException) : base(message, innerException)
        {
        }

        public SchemaMigrationException(AutoCreate autoCreate, IEnumerable<ISchemaObjectDelta> invalids) : base($"Cannot derive schema migrations for {invalids.Select(x => x.ToString()).Join(", ")} AutoCreate.{autoCreate}")
        {
            
        }
    }
}