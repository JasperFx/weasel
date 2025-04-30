using JasperFx.Core;
using Npgsql;
using Shouldly;
using Weasel.Core;
using Weasel.Postgresql.Tests.Migrations;
using Xunit;

namespace Weasel.Postgresql.Tests;

[Collection("descriptors")]
public class generating_database_descriptor : IntegrationContext
{
    public generating_database_descriptor() : base("descriptors")
    {
    }

    [Fact]
    public void can_generate_the_descriptor()
    {
        var database = new DatabaseWithTables("Foo", NpgsqlDataSource.Create(ConnectionSource.ConnectionString));

        var descriptor = database.Describe();

        descriptor.ShouldNotBeNull();
        descriptor.Properties.Any(x => x.Name.ContainsIgnoreCase("password")).ShouldBeFalse();
        descriptor.Properties.Any(x => x.Name.ContainsIgnoreCase("certificate")).ShouldBeFalse();
        descriptor.Properties.Any().ShouldBeTrue();
    }
}
