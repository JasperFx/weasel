namespace Weasel.Core;

public interface INamed
{
    string Name { get; }
}

public interface ITableColumn : INamed
{
    bool AllowNulls { get; set; }
    string? DefaultExpression { get; set; }
    string Type { get; set; }
    bool IsPrimaryKey { get; }
}

public interface ITable : ISchemaObject
{
    IReadOnlyList<string> PrimaryKeyColumns { get; }
    string PrimaryKeyName { get; set; }
    bool HasColumn(string columnName);
    void RemoveColumn(string columnName);

    ITableColumn AddColumn(string name, string columnType);
    ITableColumn AddColumn(string name, Type dotnetType);

    ITableColumn AddPrimaryKeyColumn(string name, string columnType);
    ITableColumn AddPrimaryKeyColumn(string name, Type dotnetType);
}
