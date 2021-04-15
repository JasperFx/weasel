using Baseline.ImTools;

namespace Weasel.Postgresql.Tables
{
    public interface IIndexDefinition
    {
        string IndexName { get; }

        string ToDDL(Table parent);

        bool Matches(ActualIndex index);
    }
}
