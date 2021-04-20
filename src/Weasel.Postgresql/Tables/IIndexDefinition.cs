using System;
using Baseline.ImTools;

namespace Weasel.Postgresql.Tables
{
    public interface IIndexDefinition : INamed
    {
        string ToDDL(Table parent);
    }
}
