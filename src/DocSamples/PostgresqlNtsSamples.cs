using NetTopologySuite.Geometries;
using Npgsql;
using Weasel.Core;
using Weasel.Postgresql;
using Weasel.Postgresql.Tables;

namespace DocSamples;

public class PostgresqlNtsSamples
{
    public IEnumerable<ISchemaObject> enable_postgis()
    {
        #region sample_pg_enable_postgis
        yield return new Extension("postgis");
        #endregion
    }

    public async Task configure_nts_datasource()
    {
        #region sample_pg_configure_nts_datasource
        var builder = new NpgsqlDataSourceBuilder("Host=localhost;Database=myapp;");
        builder.UseNetTopologySuite();
        await using var dataSource = builder.Build();
        #endregion
    }

    public void spatial_columns()
    {
        #region sample_pg_spatial_columns
        var table = new Table("locations");
        table.AddColumn<int>("id").AsPrimaryKey();
        table.AddColumn<string>("name").NotNull();
        table.AddColumn<Geometry>("geom");
        #endregion
    }

    public void spatial_column_with_srid()
    {
        #region sample_pg_spatial_column_with_srid
        var table = new Table("locations");
        table.AddColumn("geom", "geometry(Point, 4326)");
        #endregion
    }

    public void spatial_index()
    {
        #region sample_pg_spatial_index
        var table = new Table("locations");

        var index = new IndexDefinition("idx_locations_geom")
        {
            Method = IndexMethod.gist
        };
        index.Columns = new[] { "geom" };
        table.Indexes.Add(index);
        #endregion
    }
}
