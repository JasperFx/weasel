using JasperFx.Environment;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

internal class DatabaseConnectionCheck : IEnvironmentCheckFactory
{
    private readonly IEnumerable<IDatabase> _databases;
    private readonly IEnumerable<IDatabaseSource> _sources;

    public DatabaseConnectionCheck(IEnumerable<IDatabase> databases, IEnumerable<IDatabaseSource> sources)
    {
        _databases = databases;
        _sources = sources;
    }

    public async ValueTask<IReadOnlyList<IEnvironmentCheck>> Build()
    {
        var list = _databases.Select(x => new AssertConnectionCheck(x)).ToList();
        foreach (var source in _sources)
        {
            var databases = await source.BuildDatabases().ConfigureAwait(false);
            list.AddRange(databases.Select(x => new AssertConnectionCheck(x)));
        }

        return list.ToArray();
    }

    internal class AssertConnectionCheck: IEnvironmentCheck
    {
        private readonly IDatabase _database;

        public AssertConnectionCheck(IDatabase database)
        {
            _database = database;
        }

        public Task Assert(IServiceProvider services, CancellationToken cancellation)
        {
            return _database.AssertConnectivityAsync(cancellation);
        }

        public string Description => "Validating connectivity of Weasel database " + _database.Identifier;
    }


}
