# JSON Support

Weasel.Sqlite stores JSON data in `TEXT` columns and leverages the SQLite JSON1 extension for querying and indexing.

## Verifying JSON1 Availability

Most SQLite builds include JSON1 by default. Verify with:

```sql
SELECT json('{"test": true}');
```

If this returns `{"test":true}`, JSON1 is available.

## JSON Columns

JSON data is stored as `TEXT`. Use a raw type string when adding a JSON column:

<!-- snippet: sample_sqlite_json_columns -->
<a id='snippet-sample_sqlite_json_columns'></a>
```cs
var table = new Table("products");
table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
table.AddColumn<string>("name").NotNull();
table.AddColumn("metadata", "TEXT"); // JSON column
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L222-L227' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_json_columns' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Querying JSON Data

Use `json_extract()` in SQL to access values within JSON columns:

```sql
-- Extract a scalar value
SELECT id, json_extract(metadata, '$.category') as category
FROM products;

-- Filter on a JSON field
SELECT * FROM products
WHERE json_extract(metadata, '$.price') > 50.0;

-- Nested paths
SELECT json_extract(metadata, '$.dimensions.width') as width
FROM products;
```

## Expression Indexes on JSON Paths

Create indexes on JSON paths using `ForJsonPath()` on `IndexDefinition` for efficient queries:

<!-- snippet: sample_sqlite_json_expression_indexes -->
<a id='snippet-sample_sqlite_json_expression_indexes'></a>
```cs
var table = new Table("products");
table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
table.AddColumn<string>("name").NotNull();
table.AddColumn("metadata", "TEXT");

// Index on a JSON field
var categoryIdx = new IndexDefinition("idx_products_category");
categoryIdx.ForJsonPath("metadata", "$.category");
table.Indexes.Add(categoryIdx);

// Unique index on a JSON field
var skuIdx = new IndexDefinition("idx_products_sku") { IsUnique = true };
skuIdx.ForJsonPath("metadata", "$.sku");
table.Indexes.Add(skuIdx);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L232-L247' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_json_expression_indexes' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This generates an expression index using `json_extract()`:

```sql
CREATE INDEX "idx_products_category" ON "products" (json_extract(metadata, '$.category'));
```

## JSON Views

Combine JSON extraction with [views](/sqlite/views) for a clean query interface:

<!-- snippet: sample_sqlite_json_views -->
<a id='snippet-sample_sqlite_json_views'></a>
```cs
var view = new View("product_details", @"
SELECT id, name,
json_extract(metadata, '$.category') as category,
json_extract(metadata, '$.price') as price,
json_extract(metadata, '$.in_stock') as in_stock
FROM products");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L252-L259' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_json_views' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Full Example

<!-- snippet: sample_sqlite_json_full_example -->
<a id='snippet-sample_sqlite_json_full_example'></a>
```cs
var table = new Table("events");
table.AddColumn<int>("id").AsPrimaryKey().AutoIncrement();
table.AddColumn<string>("type").NotNull();
table.AddColumn("payload", "TEXT"); // JSON data
table.AddColumn<string>("created_at").NotNull();

// Index for filtering events by JSON field
var idx = new IndexDefinition("idx_events_source");
idx.ForJsonPath("payload", "$.source");
table.Indexes.Add(idx);

// Generate and execute DDL
var migrator = new SqliteMigrator();
var writer = new StringWriter();
table.WriteCreateStatement(migrator, writer);

var connection = new SqliteConnection("Data Source=myapp.db");
await connection.OpenAsync();
var cmd = connection.CreateCommand();
cmd.CommandText = writer.ToString();
await cmd.ExecuteNonQueryAsync();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/SqliteSamples.cs#L264-L286' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_sqlite_json_full_example' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Notes

- JSON1 functions include `json()`, `json_extract()`, `json_insert()`, `json_replace()`, `json_remove()`, `json_type()`, `json_array()`, and `json_object()`
- JSON data is stored as plain TEXT with no automatic validation unless you use `json()` or CHECK constraints
- Expression indexes on JSON paths allow SQLite to use indexed lookups for `json_extract()` queries
- STRICT mode tables require TEXT type for JSON columns (which is the default mapping)
