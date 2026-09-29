namespace Weasel.Firebird;

/// <summary>
///     The existence queries every guarded Firebird statement is conditioned on, one per kind of
///     object. Each takes names as the catalog stores them (see <see cref="SchemaUtils.CatalogName" />).
/// </summary>
/// <remarks>
///     <para>
///         Names are compared as-is, never through <c>TRIM(…)</c>: the <c>RDB$…_NAME</c> columns are
///         blank-padded <c>CHAR</c>, which compares equal to the unpadded literal, and a bare comparison
///         is what lets the server use the catalog's own index.
///     </para>
///     <para>
///         Each probe looks for the object the statement would create -- that kind, on that table -- and
///         not merely for the name. Constraint and index names share one namespace in a Firebird
///         database, and a probe that matched a primary key's own index, or a view, by name would skip
///         the statement silently and leave drift no later migration could close. Scoped, a real clash
///         reaches the server and fails loudly instead.
///     </para>
/// </remarks>
internal static class CatalogProbes
{
    /// <summary>
    ///     Any relation of that name, table or view.
    /// </summary>
    public static string Relation(string catalogName)
        => $"SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = {FirebirdScript.Literal(catalogName)}";

    public static string Table(string catalogName)
        => $"SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = {FirebirdScript.Literal(catalogName)} AND RDB$VIEW_BLR IS NULL";

    /// <summary>
    ///     An index of that name on that table which backs no constraint -- the kind a model declares.
    /// </summary>
    public static string Index(string tableCatalogName, string indexCatalogName)
        => $"SELECT 1 FROM RDB$INDICES i WHERE i.RDB$INDEX_NAME = {FirebirdScript.Literal(indexCatalogName)} AND i.RDB$RELATION_NAME = {FirebirdScript.Literal(tableCatalogName)} AND NOT EXISTS (SELECT 1 FROM RDB$RELATION_CONSTRAINTS rc WHERE rc.RDB$INDEX_NAME = i.RDB$INDEX_NAME)";

    public static string ForeignKey(string tableCatalogName, string constraintCatalogName)
        => Constraint(tableCatalogName, constraintCatalogName, "FOREIGN KEY");

    public static string Constraint(string tableCatalogName, string constraintCatalogName, string type)
        => $"SELECT 1 FROM RDB$RELATION_CONSTRAINTS WHERE RDB$CONSTRAINT_NAME = {FirebirdScript.Literal(constraintCatalogName)} AND RDB$RELATION_NAME = {FirebirdScript.Literal(tableCatalogName)} AND RDB$CONSTRAINT_TYPE = {FirebirdScript.Literal(type)}";

    public static string PrimaryKey(string tableCatalogName)
        => $"SELECT 1 FROM RDB$RELATION_CONSTRAINTS WHERE RDB$RELATION_NAME = {FirebirdScript.Literal(tableCatalogName)} AND RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'";

    public static string Column(string tableCatalogName, string columnCatalogName)
        => $"SELECT 1 FROM RDB$RELATION_FIELDS WHERE RDB$RELATION_NAME = {FirebirdScript.Literal(tableCatalogName)} AND RDB$FIELD_NAME = {FirebirdScript.Literal(columnCatalogName)}";

    public static string Sequence(string catalogName)
        => $"SELECT 1 FROM RDB$GENERATORS WHERE RDB$GENERATOR_NAME = {FirebirdScript.Literal(catalogName)}";

    public static string View(string catalogName)
        => $"SELECT 1 FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = {FirebirdScript.Literal(catalogName)} AND RDB$VIEW_BLR IS NOT NULL";

}
