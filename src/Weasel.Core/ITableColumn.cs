namespace Weasel.Core;

public interface ITableColumn
{
    bool AllowNulls { get; set; }
    string? DefaultExpression { get; set; }
    string Type { get; set; }
    bool IsPrimaryKey { get; }
    bool IsAutoNumber { get; set; }
}
