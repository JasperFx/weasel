using System.Linq.Expressions;
using JasperFx.Events;
using Shouldly;
using Weasel.Storage.Flattened;
using Xunit;

namespace Weasel.Core.Tests.Flattened;

/// <summary>
///     Reading a mapping lambda: the column it names by default, and the value it pulls off an event.
/// </summary>
public class flat_table_member_mapping
{
    public record Address(string City);

    public record Shipped(decimal Amount, Guid CustomerId, Address Destination);

    private static Shipped AnEvent() => new(12.5m, Guid.Parse("22222222-2222-2222-2222-222222222222"),
        new Address("Austin"));

    [Theory]
    [InlineData("MemberCount", "member_count")]
    [InlineData("A", "a")]
    [InlineData("Amount", "amount")]
    [InlineData("CustomerId", "customer_id")]
    public void snake_case(string member, string expected)
        => FlatTableMembers.SnakeCase(member).ShouldBe(expected);

    [Fact]
    public void the_default_column_name_of_a_nested_path_joins_the_whole_path()
    {
        // A leaf-only name would silently collide whenever two mapped paths end in the same member.
        var path = FlatTableMembers.MemberPath(
            (Expression<Func<Shipped, object>>)(x => x.Destination.City))!;

        FlatTableMembers.DefaultColumnName(path).ShouldBe("destination_city");
    }

    [Fact]
    public void the_default_column_name_of_a_single_member()
    {
        var member = FlatTableMembers.MemberOf((Shipped x) => x.Amount);

        FlatTableMembers.DefaultColumnName([member]).ShouldBe("amount");
    }

    [Fact]
    public void an_expression_that_is_not_a_member_access_is_refused_by_name()
    {
        var ex = Should.Throw<ArgumentException>(() =>
            FlatTableMembers.MemberOf((Shipped x) => x.Amount + 1));

        ex.Message.ShouldContain("member access");
    }

    [Fact]
    public void a_member_setter_reads_the_event_body()
    {
        var setter = FlatTableParameterSetters.ForMember((Shipped x) => x.Amount);

        setter.ValueFor(new Event<Shipped>(AnEvent())).ShouldBe(12.5m);
    }

    [Fact]
    public void a_member_setter_walks_a_nested_path()
    {
        var path = FlatTableMembers.MemberPath(
            (Expression<Func<Shipped, object>>)(x => x.Destination.City))!;

        FlatTableParameterSetters.ForMembers<Shipped>(path)
            .ValueFor(new Event<Shipped>(AnEvent()))
            .ShouldBe("Austin");
    }

    [Fact]
    public void the_store_supplied_conversion_is_applied()
    {
        // SQLite has to write a Guid as lowercase canonical text and SQL Server has to unwrap a
        // strong-typed id; neither is Weasel's decision, so the conversion is the store's to supply.
        var setter = FlatTableParameterSetters.ForMember((Shipped x) => x.CustomerId,
            value => value?.ToString());

        setter.ValueFor(new Event<Shipped>(AnEvent()))
            .ShouldBe("22222222-2222-2222-2222-222222222222");
    }

    [Fact]
    public void a_stream_keyed_table_takes_its_key_from_the_stores_stream_identity()
    {
        var streamId = Guid.Parse("11111111-1111-1111-1111-111111111111");

        var @event = new Event<Shipped>(AnEvent()) { StreamId = streamId, StreamKey = "shipment-1" };

        FlatTableParameterSetters.ForStream(StreamIdentity.AsGuid).ValueFor(@event).ShouldBe(streamId);
        FlatTableParameterSetters.ForStream(StreamIdentity.AsString).ValueFor(@event).ShouldBe("shipment-1");

        FlatTableParameterSetters.ForStream(StreamIdentity.AsGuid, value => value?.ToString())
            .ValueFor(@event)
            .ShouldBe("11111111-1111-1111-1111-111111111111");
    }
}
