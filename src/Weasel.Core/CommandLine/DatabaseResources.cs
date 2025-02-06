using JasperFx.Resources;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

internal class DatabaseResources: IStatefulResourceSource
{
    private readonly IEnumerable<IDatabase> _databases;
    private readonly IEnumerable<IDatabaseSource> _sources;


    public DatabaseResources(IEnumerable<IDatabase> databases, IEnumerable<IDatabaseSource> sources)
    {
        _databases = databases;
        _sources = sources;
    }

    public async ValueTask<IReadOnlyList<IStatefulResource>> FindResources()
    {
        var list = new List<IStatefulResource>();
        list.AddRange(_databases.Select(x => new DatabaseResource(x)));

        foreach (var source in _sources)
        {
            var databases = await source.BuildDatabases().ConfigureAwait(false);
            list.AddRange(databases.Select(x => new DatabaseResource(x)));
        }

        return list;
    }
}
