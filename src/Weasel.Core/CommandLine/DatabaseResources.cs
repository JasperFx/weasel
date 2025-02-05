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

    public ValueTask<IReadOnlyList<IStatefulResource>> FindResources()
    {
        var list = new List<IStatefulResource>();
        list.AddRange(_databases.Select(x => new DatabaseResource(x)));

        foreach (var source in _sources)
        {
            // BOO! Reevaluate this in JasperFx some day, but not right now.
#pragma warning disable VSTHRD103
            var databases = source.BuildDatabases().AsTask().GetAwaiter().GetResult();
#pragma warning restore VSTHRD103
            list.AddRange(databases.Select(x => new DatabaseResource(x)));
        }

        return ValueTask.FromResult<IReadOnlyList<IStatefulResource>>(list);
    }

}
