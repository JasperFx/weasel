# NetTopologySuite

Weasel.Postgresql includes built-in support for spatial types via [NetTopologySuite](https://github.com/NetTopologySuite/NetTopologySuite) and the PostGIS extension.

## Prerequisites

Ensure PostGIS is installed in your database and the Npgsql NetTopologySuite plugin is configured:

```bash
dotnet add package Npgsql.NetTopologySuite
dotnet add package Weasel.Postgresql
```

Enable the PostGIS extension in your database schema:

<!-- snippet: sample_pg_enable_postgis -->
<a id='snippet-sample_pg_enable_postgis'></a>
```cs
yield return new Extension("postgis");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlNtsSamples.cs#L13-L15' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_enable_postgis' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## NpgsqlDataSource Configuration

Register the NetTopologySuite type mappings when building your data source:

<!-- snippet: sample_pg_configure_nts_datasource -->
<a id='snippet-sample_pg_configure_nts_datasource'></a>
```cs
var builder = new NpgsqlDataSourceBuilder("Host=localhost;Database=myapp;");
builder.UseNetTopologySuite();
await using var dataSource = builder.Build();
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlNtsSamples.cs#L20-L24' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_configure_nts_datasource' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Type Mappings

Weasel automatically maps `NetTopologySuite.Geometries.Geometry` (and its subclasses) to the PostgreSQL `geometry` type. This mapping is included in the default `NpgsqlTypeMapper`.

| .NET Type | PostgreSQL Type |
|-----------|----------------|
| `Geometry` | `geometry` |
| `Point` | `geometry` |
| `LineString` | `geometry` |
| `Polygon` | `geometry` |
| `MultiPoint` | `geometry` |
| `MultiLineString` | `geometry` |
| `MultiPolygon` | `geometry` |
| `GeometryCollection` | `geometry` |

## Using Spatial Columns

Define geometry columns on your tables:

<!-- snippet: sample_pg_spatial_columns -->
<a id='snippet-sample_pg_spatial_columns'></a>
```cs
var table = new Table("locations");
table.AddColumn<int>("id").AsPrimaryKey();
table.AddColumn<string>("name").NotNull();
table.AddColumn<Geometry>("geom");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlNtsSamples.cs#L29-L34' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_spatial_columns' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This generates a column of type `geometry`. For more specific PostGIS types, use the string overload:

<!-- snippet: sample_pg_spatial_column_with_srid -->
<a id='snippet-sample_pg_spatial_column_with_srid'></a>
```cs
var table = new Table("locations");
table.AddColumn("geom", "geometry(Point, 4326)");
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlNtsSamples.cs#L39-L42' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_spatial_column_with_srid' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Spatial Indexes

Create a GiST index for spatial queries:

<!-- snippet: sample_pg_spatial_index -->
<a id='snippet-sample_pg_spatial_index'></a>
```cs
var table = new Table("locations");

var index = new IndexDefinition("idx_locations_geom")
{
    Method = IndexMethod.gist
};
index.Columns = new[] { "geom" };
table.Indexes.Add(index);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/PostgresqlNtsSamples.cs#L47-L56' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_pg_spatial_index' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This generates:

```sql
CREATE INDEX idx_locations_geom ON public.locations USING gist (geom);
```
