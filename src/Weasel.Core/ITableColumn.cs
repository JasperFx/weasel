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
