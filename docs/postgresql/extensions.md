# Extensions

The `Extension` class ensures that a PostgreSQL extension is installed in the database. Extensions are common for adding capabilities like UUID generation, full-text search languages, or spatial types.

## Registering an Extension

<!-- snippet: sample_pg_register_extensions -->
<a id='snippet-sample_pg_register_extensions'></a>
```cs
var uuidExt = new Extension("uuid-ossp");
var plv8Ext = new Extension("plv8");
var postgisExt = new Extension("postgis");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlExtensionSamples.cs#L13-L17' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_register_extensions' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

Extension names are automatically normalized to lowercase and trimmed.

## Using with a Database

Include extensions in your `PostgresqlDatabase` subclass so they are created before tables and functions that depend on them.

<!-- snippet: sample_pg_database_with_extensions -->
<a id='snippet-sample_pg_database_with_extensions'></a>
```cs
public class AppDatabaseWithExtensions : PostgresqlDatabase
{
    public AppDatabaseWithExtensions(NpgsqlDataSource dataSource)
        : base(new DefaultMigrationLogger(), AutoCreate.CreateOrUpdate,
               new PostgresqlMigrator(), "app", dataSource)
    {
    }

    public override IFeatureSchema[] BuildFeatureSchemas()
    {
        return [new AppFeatureSchema(this)];
    }

    private class AppFeatureSchema : FeatureSchemaBase
    {
        public AppFeatureSchema(AppDatabaseWithExtensions database)
            : base("App", database.Migrator)
        {
        }

        public override Type StorageType => typeof(AppDatabaseWithExtensions);

        protected override IEnumerable<ISchemaObject> schemaObjects()
        {
            // Extensions should be listed first
            yield return new Extension("uuid-ossp");
            yield return new Extension("postgis");

            // Then tables, functions, etc.
            var table = new Weasel.Postgresql.Tables.Table("users");
            table.AddColumn<int>("id").AsPrimaryKey();
            yield return table;
        }
    }
}
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlExtensionSamples.cs#L20-L56' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_database_with_extensions' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

Weasel queries `pg_extension` to check whether the extension is already installed. If not, it returns a `Create` delta.

<!-- snippet: sample_pg_extension_delta_detection -->
<a id='snippet-sample_pg_extension_delta_detection'></a>
```cs
var ext = new Extension("hstore");

// Used internally during migration, but you can invoke manually:
var migrator = new PostgresqlMigrator();
var writer = new StringWriter();
ext.WriteCreateStatement(migrator, writer);
// CREATE EXTENSION IF NOT EXISTS hstore;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlExtensionSamples.cs#L60-L68' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_extension_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

<!-- snippet: sample_pg_extension_generate_ddl -->
<a id='snippet-sample_pg_extension_generate_ddl'></a>
```cs
var ext = new Extension("uuid-ossp");
var migrator = new PostgresqlMigrator();
var writer = new StringWriter();

ext.WriteCreateStatement(migrator, writer);
// CREATE EXTENSION IF NOT EXISTS uuid-ossp;

ext.WriteDropStatement(migrator, writer);
// DROP EXTENSION IF EXISTS uuid-ossp CASCADE;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlExtensionSamples.cs#L73-L83' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_extension_generate_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

::: warning
Creating extensions requires the `CREATE` privilege on the database. In managed hosting environments (e.g., AWS RDS, Azure), some extensions may need to be enabled through the hosting provider's console.
:::
