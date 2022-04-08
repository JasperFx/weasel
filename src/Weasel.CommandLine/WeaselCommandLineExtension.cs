using Microsoft.Extensions.DependencyInjection;
using Oakton;
using Oakton.Environment;
using Weasel.Core.Migrations;

namespace Weasel.CommandLine;

internal class WeaselCommandLineExtension : IServiceRegistrations
{
    public void Configure(IServiceCollection services)
    {
        services.AddSingleton<IEnvironmentCheckFactory, DatabaseConnectionCheck>();
    }
}

public class DatabaseConnectionCheck : IEnvironmentCheckFactory
{
    private readonly IEnumerable<IDatabase> _databases;
    private readonly IEnumerable<IDatabaseSource> _sources;

    public DatabaseConnectionCheck(IEnumerable<IDatabase> databases, IEnumerable<IDatabaseSource> sources)
    {
        _databases = databases;
        _sources = sources;
    }

    public IEnvironmentCheck[] Build()
    {
        var list = _databases.Select(x => new AssertConnectionCheck(x)).ToList();
        foreach (var source in _sources)
        {
            var databases = source.BuildDatabases().AsTask().ConfigureAwait(false).GetAwaiter().GetResult();
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
            return _database.AssertConnectivity();
        }

        public string Description => "Validating connectivity of Weasel database " + _database.Identifier;
    }


}

