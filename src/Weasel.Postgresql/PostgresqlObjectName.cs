using Weasel.Core;

namespace Weasel.Postgresql;

public class PostgresqlObjectName: DbObjectName
{
    public PostgresqlObjectName(string schema, string name)
        : base(schema, name, PostgresqlProvider.Instance.ToQualifiedName(schema, name))
    {
    }
    public PostgresqlObjectName(string schema, string name, string qualifiedName)
        : base(schema, name, qualifiedName)
    {
    }
}
