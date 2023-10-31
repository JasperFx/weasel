using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.SqlServer;

public class SqlServerObjectName: DbObjectName
{
    public SqlServerObjectName(string schema, string name)
        : base(schema, name, SqlServerProvider.Instance.As<IDatabaseProvider>().ToQualifiedName(schema, name))
    {
    }
}
