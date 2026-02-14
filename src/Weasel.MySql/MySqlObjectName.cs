using JasperFx.Core;
using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.MySql;

public class MySqlObjectName: DbObjectName
{
    protected override string QuotedQualifiedName => Schema.IsEmpty()
        ? $"`{Name}`"
        : $"`{Schema}`.`{Name}`";

    public MySqlObjectName(string schema, string name)
        : base(schema, name, ComputeQualifiedName(schema, name))
    {
    }

    private static string ComputeQualifiedName(string schema, string name)
    {
        return schema.IsEmpty()
            ? $"`{name}`"
            : $"`{schema}`.`{name}`";
    }

    private MySqlObjectName(DbObjectName dbObjectName): this(dbObjectName.Schema, dbObjectName.Name)
    {
    }

    public static MySqlObjectName From(DbObjectName dbObjectName) =>
        new MySqlObjectName(dbObjectName);

    public string ToIndexName(string prefix, params string[] columnNames)
    {
        var name = $"{prefix}_{Name}_{string.Join("_", columnNames)}";
        return name.Length > 64 ? name.Substring(0, 64) : name;
    }

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
            return (typeof(DbObjectName).GetHashCode() * 397) ^ QualifiedName.GetHashCode();
        }
    }
}
