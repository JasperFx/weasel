namespace Weasel.Firebird;

/// <summary>
///     The existence queries every guarded Firebird statement is conditioned on, one per kind of
///     object. Each takes the name as the catalog stores it (see <see cref="SchemaUtils.CatalogName" />).
/// </summary>
/// <remarks>
///     The name is compared as-is, never through <c>TRIM(…)</c>: the <c>RDB$…_NAME</c> columns are
///     blank-padded <c>CHAR</c>, which compares equal to the unpadded literal, and a bare comparison is
///     what lets the server use the catalog's own index.
/// </remarks>
internal static class CatalogProbes
{
    public static string Relation(string catalogName)
        => $"SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = {FirebirdScript.Literal(catalogName)}";

    public static string Index(string catalogName)
        => $"SELECT 1 FROM RDB$INDICES WHERE RDB$INDEX_NAME = {FirebirdScript.Literal(catalogName)}";

    public static string Constraint(string catalogName)
        => $"SELECT 1 FROM RDB$RELATION_CONSTRAINTS WHERE RDB$CONSTRAINT_NAME = {FirebirdScript.Literal(catalogName)}";

    public static string PrimaryKey(string tableCatalogName)
        => $"SELECT 1 FROM RDB$RELATION_CONSTRAINTS WHERE RDB$RELATION_NAME = {FirebirdScript.Literal(tableCatalogName)} AND RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'";

    public static string Column(string tableCatalogName, string columnCatalogName)
        => $"SELECT 1 FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = {FirebirdScript.Literal(tableCatalogName)} AND RDB$FIELD_NAME = {FirebirdScript.Literal(columnCatalogName)}";

    public static string Sequence(string catalogName)
        => $"SELECT 1 FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME = {FirebirdScript.Literal(catalogName)}";
}
