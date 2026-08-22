using JasperFx.Core.Reflection;
using Weasel.Core;

namespace Weasel.Oracle;

public class OracleObjectName: DbObjectName
{
    protected override string QuotedQualifiedName => $"{SchemaUtils.QuoteName(Schema)}.{SchemaUtils.QuoteName(Name)}";

    /// <summary>
    ///     A name can arrive already delimited -- <c>QualifiedNameParser</c> keeps the parts of a
    ///     qualified name exactly as written, and Weasel emitted most identifiers bare until 9.25, so
    ///     delimiting one by hand was the only way to use it. The model has to hold the spelling the
    ///     catalog reports, because that is what introspection binds; holding the delimited spelling
    ///     matched nothing, so the object read as absent and was recreated on every run (weasel#499).
    /// </summary>
    public OracleObjectName(string schema, string name)
        : base(SchemaUtils.Unquote(schema), SchemaUtils.Unquote(name),
            OracleProvider.Instance.As<IDatabaseProvider>()
                .ToQualifiedName(SchemaUtils.Unquote(schema), SchemaUtils.Unquote(name)))
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
