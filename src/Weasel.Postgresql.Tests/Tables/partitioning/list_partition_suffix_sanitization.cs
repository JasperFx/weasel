using System.IO;
using Shouldly;
using Weasel.Postgresql.Tables;
using Weasel.Postgresql.Tables.Partitioning;
using Xunit;

namespace Weasel.Postgresql.Tests.Tables.partitioning;

// A partition table is named `{parent}_{suffix}` as an unquoted Postgres identifier. A suffix taken
// straight from a tenant id can contain '-' (and other non-identifier characters), which produced
// invalid DDL (`CREATE TABLE ..._tenant-a` → 42601). The suffix is now sanitized to [a-z0-9_] while
// the partition VALUE keeps the exact tenant id. DB-free — exercises the rendered DDL directly.
public class list_partition_suffix_sanitization
{
    [Theory]
    [InlineData("tenant-a", "tenant_a")]
    [InlineData("Tenant-A", "tenant_a")]
    [InlineData("a.b:c@d", "a_b_c_d")]
    [InlineData("already_valid", "already_valid")]
    [InlineData("b_1", "b_1")]
    public void suffix_is_normalized_to_a_valid_identifier(string input, string expected)
    {
        new ListPartition(input).Suffix.ShouldBe(expected);
    }

    [Fact]
    public void sanitization_is_idempotent_so_the_parse_round_trip_stays_symmetric()
    {
        var once = new ListPartition("tenant-a").Suffix;
        new ListPartition(once).Suffix.ShouldBe(once);
    }

    [Fact]
    public void create_statement_uses_a_valid_table_name_but_keeps_the_exact_partition_value()
    {
        var table = new Table("public.mt_streams");
        table.AddColumn<string>("tenant_id");

        var writer = new StringWriter();
        ((IPartition)new ListPartition("tenant-a", "'tenant-a'")).WriteCreateStatement(writer, table);
        var ddl = writer.ToString();

        ddl.ShouldContain("mt_streams_tenant_a");        // sanitized, valid unquoted identifier
        ddl.ShouldNotContain("mt_streams_tenant-a");     // the invalid hyphenated form is gone
        ddl.ShouldContain("for values in ('tenant-a')"); // the partition VALUE is still the exact tenant id
    }
}
