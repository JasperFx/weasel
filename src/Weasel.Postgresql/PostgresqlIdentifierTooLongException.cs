using System;
using System.Runtime.Serialization;

namespace Weasel.Postgresql
{
    public class PostgresqlIdentifierTooLongException: Exception
    {
        public int Length { get; set; }
        public string Name { get; set; }

        public PostgresqlIdentifierTooLongException(int length, string name)
            : base($"Database identifier {name} would be truncated. The {nameof(PostgresqlMigrator)}{nameof(PostgresqlMigrator.NameDataLength)} property is currently {length}. You may want to change this value with a corresponding change to Postgresql's NAMEDATALEN or by explicitly overriding the database object name that is violating the length rule")
        {
            Length = length;
            Name = name;
        }

        protected PostgresqlIdentifierTooLongException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
