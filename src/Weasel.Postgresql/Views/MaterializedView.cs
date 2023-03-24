using Weasel.Core;

namespace Weasel.Postgresql.Views;

public class MaterializedView: View
{
    public MaterializedView(string viewName, string viewSql): base(viewName, viewSql)
    {
    }

    public MaterializedView(DbObjectName name, string viewSql): base(name, viewSql)
    {
    }

    protected override string ViewType => "MATERIALIZED VIEW";

    protected override char ViewKind => 'm';

    protected override string GetCreationOptions() => string.IsNullOrEmpty(AccessMethod) ? string.Empty : $"USING {AccessMethod}";

    public string? AccessMethod { get; set; }

    public MaterializedView UseAccessMethod(string accessMethod)
    {
        AccessMethod = accessMethod;
        return this;
    }
}

