# Functions and Extensions

SQLite has no `CREATE FUNCTION`. Application-defined functions are registered on a connection
handle through `Microsoft.Data.Sqlite`, and loadable extensions are likewise attached per
connection. Both are gone the moment the connection closes. That makes them a per-open concern
rather than a schema object, and the place Weasel handles them is `SqliteDataSource`.

## Registering functions through the data source

Hand a `SqliteFunctionRegistry` (and optionally a `SqliteExtensionSettings`) to `SqliteDataSource`
and every connection it opens, synchronously or asynchronously, gets the same functions:

<!-- snippet: sample_sqlite_data_source_functions -->
<a id='snippet-sample_sqlite_data_source_functions'></a>
```cs
var functions = new SqliteFunctionRegistry();
functions.AddScalar<double>("cosine_distance", (object? a, object? b) =>
{
    var x = (byte[])a!;
    var y = (byte[])b!;
    double dot = 0, nx = 0, ny = 0;
    for (var i = 0; i + 3 < x.Length; i += 4)
    {
        var xi = BitConverter.ToSingle(x, i);
        var yi = BitConverter.ToSingle(y, i);
        dot += xi * yi;
        nx += xi * xi;
        ny += yi * yi;
    }

    return 1 - dot / (Math.Sqrt(nx) * Math.Sqrt(ny));
});

var extensions = new SqliteExtensionSettings();
// extensions.AddExtension("vec0"); // a native extension, if you need one

await using var dataSource = new SqliteDataSource(
    "Data Source=myapp.db",
    SqlitePragmaSettings.Default,
    functions,
    extensions);

// every connection the data source opens has cosine_distance() registered
await using var connection = await dataSource.OpenConnectionAsync();
await using var cmd = connection.CreateCommand();
cmd.CommandText = "select id from vectors order by cosine_distance(embedding, @q) limit 10";
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L453-L485' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_data_source_functions' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

The registry is live. Functions added to `dataSource.Functions` after construction are registered
on connections opened from then on, so a library can build the data source first and let the
application contribute functions later.

The per-open order is connection-scoped PRAGMAs, then extensions, then functions, so a managed
function can build on something an extension provides.

## Why not register on a connection directly?

You can, and the `Microsoft.Data.Sqlite` API for it is `connection.CreateFunction(...)`. The
trap is that a function registered on one connection does not exist on the next one, and a
connection pool hands out whichever is free. A function that "sometimes isn't there" is almost
always a function registered on a connection instead of on the data source that produces them.

## Extensions

`SqliteExtensionSettings.AddExtension(libraryPath, entryPoint?)` names a native extension to load.
Extension loading is enabled only for the duration of the loads and disabled again afterwards. A
library that cannot be loaded fails the open with the library path in the message, and the
connection is disposed rather than handed out half-configured.

The bundled `e_sqlite3` build that `Microsoft.Data.Sqlite` ships with already has FTS5, JSON and
R*Tree compiled in, so many applications never need an extension at all; a registered managed
function covers cases like a cosine distance over a BLOB with no native dependency.
