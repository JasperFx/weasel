using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Oracle;

public class OracleObjectName: DbObjectName
{
    protected override string QuotedQualifiedName => $"{SchemaUtils.QuoteName(Schema)}.{SchemaUtils.QuoteName(Name)}";

    public OracleObjectName(string schema, string name)
        : base(schema, name, OracleProvider.Instance.As<IDatabaseProvider>().ToQualifiedName(schema, name))
    {
    }

    private OracleObjectName(DbObjectName dbObjectName): this(dbObjectName.Schema, dbObjectName.Name)
    {
    }

    public static OracleObjectName From(DbObjectName dbObjectName) =>
        new OracleObjectName(dbObjectName);

    private new bool Equals(DbObjectName other)
    {
        return string.Equals(QualifiedName, other.QualifiedName, StringComparison.OrdinalIgnoreCase);
    }

    public override bool Equals(object? obj)
    {
        if (obj is DbObjectName dbObjectName)
        {
            return Equals(dbObjectName);
        }

        return base.Equals(obj);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            return (typeof(DbObjectName).GetHashCode() * 397) ^ QualifiedName.ToUpperInvariant().GetHashCode();
        }
    }
}
