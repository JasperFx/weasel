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
        descriptor.ServerName.ShouldBe("localhost");
        descriptor.Properties.Any(x => x.Name.ContainsIgnoreCase("password")).ShouldBeFalse();
        descriptor.Properties.Any(x => x.Name.ContainsIgnoreCase("certificate")).ShouldBeFalse();
        descriptor.Properties.Any().ShouldBeTrue();
    }

    [Fact]
    public void can_generate_the_descriptor_multihost()
    {
        var database = new DatabaseWithTables("Foo", NpgsqlDataSource.Create("Host=localhost:5432,localhost:5444;Database=marten_testing;Username=postgres;password=postgres;SSL Mode=Disable"));

        var descriptor = database.Describe();

        descriptor.ShouldNotBeNull();
        descriptor.ServerName.ShouldBe("localhost");
        descriptor.Properties.Any(x => x.Name.ContainsIgnoreCase("password")).ShouldBeFalse();
        descriptor.Properties.Any(x => x.Name.ContainsIgnoreCase("certificate")).ShouldBeFalse();
        descriptor.Properties.Any().ShouldBeTrue();
    }

    [Fact]
    public void can_generate_the_descriptor_multihost_portlessdomains()
    {
        var database = new DatabaseWithTables("Foo", NpgsqlDataSource.Create("Host=my-db-host.com,my-db-host-ro.com;Database=marten_testing;Username=postgres;password=postgres;SSL Mode=Disable"));

        var descriptor = database.Describe();

        descriptor.ShouldNotBeNull();
        descriptor.ServerName.ShouldBe("my-db-host.com");
        descriptor.Properties.Any(x => x.Name.ContainsIgnoreCase("password")).ShouldBeFalse();
        descriptor.Properties.Any(x => x.Name.ContainsIgnoreCase("certificate")).ShouldBeFalse();
        descriptor.Properties.Any().ShouldBeTrue();
    }
}
