using System.Runtime.Serialization;

namespace Weasel.Postgresql;

public class PostgresqlIdentifierInvalidException: Exception
{
    public PostgresqlIdentifierInvalidException(string name)
        : base(
            $"Database identifier {name} is not valid. See https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html for valid unquoted identifiers (Marten does not quote identifiers).")
    {
        Name = name;
    }

    public string Name { get; set; }
}
