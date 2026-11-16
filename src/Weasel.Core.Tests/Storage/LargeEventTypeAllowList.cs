using JasperFx.Events;
using Weasel.Storage;

namespace Weasel.Core.Tests.StorageHelpers;

/// <summary>
///     weasel#846: an <see cref="EventTypeAllowList"/> too long to bind one parameter per name on SQL
///     Server, which refuses any request with more than 2,100 parameters. Linked into the provider test
///     projects so each can render it through a dialect against a real server.
/// </summary>
public static class LargeEventTypeAllowList
{
    /// <summary>Comfortably past SQL Server's 2,100-parameter cap.</summary>
    public const int Size = 2500;

    public static string AliasAt(int index) => $"event_type_{index:D4}";

    public static string DotNetTypeNameAt(int index) => $"Weasel.Tests.EventType{index:D4}, Weasel.Tests";

    /// <summary>
    ///     A list of <paramref name="size" /> distinct names. There are not 2,500 CLR types to hand, so the
    ///     registry hands out a fresh name on every call and the one type is passed repeatedly.
    /// </summary>
    public static EventTypeAllowList Build(int size = Size) =>
        EventTypeAllowList.For(new CountingRegistry(), Enumerable.Repeat(typeof(LargeListEvent), size).ToList());

    private sealed record LargeListEvent;

    private sealed class CountingRegistry: EventRegistry
    {
        private int _next;

        public override IEventType EventMappingFor(Type eventType)
        {
            var index = _next++;
            return new EventTypeData<object>
            {
                EventTypeName = AliasAt(index), DotNetTypeName = DotNetTypeNameAt(index)
            };
        }
    }
}
