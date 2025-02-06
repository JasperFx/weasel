using JasperFx.Environment;
using Weasel.Core.Migrations;

namespace Weasel.Core.CommandLine;

public class AssertAllWeaselDatabasesCheck: IEnvironmentCheckFactory
{
    private readonly IEnumerable<IDatabase> _databases;
    private readonly IEnumerable<IDatabaseSource> _sources;

    public AssertAllWeaselDatabasesCheck(IEnumerable<IDatabase> databases, IEnumerable<IDatabaseSource> sources)
    {
        _databases = databases;
        _sources = sources;
    }

    public async ValueTask<IReadOnlyList<IEnvironmentCheck>> Build()
    {
        var list = _databases.Select(x => new AssertDatabaseCheck(x)).ToList();
        foreach (var source in _sources)
        {
            var databases = await source.BuildDatabases().ConfigureAwait(false);
            list.AddRange(databases.Select(x => new AssertDatabaseCheck(x)));
        }

        return list.ToArray();
    }

    internal class AssertDatabaseCheck: IEnvironmentCheck
    {
        private readonly IDatabase _database;

        public AssertDatabaseCheck(IDatabase database)
        {
            _database = database;
            Description = $"Assert the expected configuration of database '{database.Identifier}'";
        }

        public Task Assert(IServiceProvider services, CancellationToken cancellation) =>
            _database.AssertDatabaseMatchesConfigurationAsync(cancellation);

        public string Description { get; }
    }


    public string Description { get; } = "Weasel: Validating the configuration of registered databases";
}
