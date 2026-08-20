namespace Weasel.Core;

/// <summary>When a trigger fires relative to the statement that caused it.</summary>
public enum TriggerTiming
{
    Before,
    After,

    /// <summary>
    ///     Replaces the statement rather than running alongside it. SQL Server and PostgreSQL use
    ///     this to make a view writable; SQLite uses it on views too. MySQL and Oracle spell the
    ///     equivalent differently or not at all.
    /// </summary>
    InsteadOf
}

/// <summary>
///     The statements a trigger fires on. A flags enum because most engines let one trigger cover
///     several — MySQL is the exception and accepts exactly one.
/// </summary>
[Flags]
public enum TriggerEvents
{
    None = 0,
    Insert = 1,
    Update = 2,
    Delete = 4,

    /// <summary>PostgreSQL only, and statement-level only.</summary>
    Truncate = 8
}

/// <summary>
///     Cross-provider base for a database trigger.
/// </summary>
/// <remarks>
///     <para>
///         <strong>A trigger is an independent schema object that declares its target, not
///         something a table owns</strong> (weasel#452). Indexes and foreign keys are table-owned
///         because they are part of the table's own definition and several are emitted inside
///         <c>CREATE TABLE</c>; a trigger is always a separate statement with its own name and
///         lifecycle. It also does not always have a table — SQL Server's <c>INSTEAD OF</c> triggers
///         attach to views, and Oracle has schema- and database-level triggers with no target object
///         at all. Table ownership would leave both homeless.
///     </para>
///     <para>
///         The accepted cost is that a trigger and its target can drift apart in the model, since
///         nothing forces them to be registered together. Views and functions already behave that
///         way, so it is consistent rather than novel.
///     </para>
///     <para>
///         <strong>SQLite has a standing hazard here.</strong> Its <c>TableDelta</c> rebuilds the
///         table for most column, foreign key and primary key changes, and dropping a table silently
///         drops its triggers — so the recreate path has to re-emit the triggers targeting the table
///         it rebuilt. That is a lookup, not ownership, and it is covered by a test, because the
///         failure mode destroys user data-integrity logic without saying anything.
///     </para>
/// </remarks>
public abstract class TriggerBase: SchemaObjectBase
{
    protected TriggerBase(DbObjectName identifier, DbObjectName target, string body) : base(identifier)
    {
        Target = target ?? throw new ArgumentNullException(nameof(target));
        Body = body ?? throw new ArgumentNullException(nameof(body));
    }

    /// <summary>
    ///     The table or view this trigger fires on.
    /// </summary>
    public DbObjectName Target { get; }

    /// <summary>
    ///     The action the trigger performs, in the provider's own dialect. PostgreSQL is the odd one
    ///     out: its body is a call to a function that must exist already, rather than a block of
    ///     statements, so a PostgreSQL trigger composes with <c>Function</c> rather than carrying
    ///     its own logic.
    /// </summary>
    public string Body { get; }

    public TriggerTiming Timing { get; set; } = TriggerTiming.Before;

    public TriggerEvents Events { get; set; } = TriggerEvents.Insert;

    /// <summary>
    ///     Fire once per affected row rather than once per statement. SQL Server has no row-level
    ///     triggers and ignores this; SQLite and MySQL are always row-level.
    /// </summary>
    public bool ForEachRow { get; set; } = true;

    /// <summary>
    ///     Optional <c>WHEN</c> condition. Not supported by every engine; a provider that cannot
    ///     emit one throws rather than dropping it silently.
    /// </summary>
    public string? Condition { get; set; }

    /// <summary>
    ///     Render <see cref="Events" /> as the dialect's event list. Ordered <c>INSERT</c>,
    ///     <c>UPDATE</c>, <c>DELETE</c>, <c>TRUNCATE</c> so that the generated text is stable and
    ///     comparable regardless of the order the flags were set in.
    /// </summary>
    /// <param name="separator">
    ///     <c>" OR "</c> for PostgreSQL and Oracle, <c>", "</c> for SQL Server.
    /// </param>
    protected string EventList(string separator)
    {
        var events = new List<string>();

        if (Events.HasFlag(TriggerEvents.Insert)) events.Add("INSERT");
        if (Events.HasFlag(TriggerEvents.Update)) events.Add("UPDATE");
        if (Events.HasFlag(TriggerEvents.Delete)) events.Add("DELETE");
        if (Events.HasFlag(TriggerEvents.Truncate)) events.Add("TRUNCATE");

        if (events.Count == 0)
        {
            throw new InvalidOperationException(
                $"Trigger {Identifier} declares no events. Set {nameof(Events)} to at least one of "
                + "Insert, Update, Delete or Truncate.");
        }

        return string.Join(separator, events);
    }

    /// <summary>
    ///     The single event this trigger fires on, for the engines that accept exactly one.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     When more than one event is set. Silently dropping the extras would leave the caller with
    ///     a trigger narrower than the one they asked for, which is the failure mode weasel#449
    ///     removed from index predicates.
    /// </exception>
    protected string SingleEvent(string providerName)
    {
        var events = EventList(",").Split(',');

        if (events.Length > 1)
        {
            throw new InvalidOperationException(
                $"{providerName} triggers fire on exactly one event, but trigger {Identifier} declares "
                + $"{string.Join(", ", events)}. Declare one trigger per event.");
        }

        return events[0];
    }

    protected string TimingKeyword()
        => Timing switch
        {
            TriggerTiming.Before => "BEFORE",
            TriggerTiming.After => "AFTER",
            TriggerTiming.InsteadOf => "INSTEAD OF",
            _ => throw new ArgumentOutOfRangeException(nameof(Timing), Timing, null)
        };

    /// <summary>
    ///     Normalize a trigger body for comparison against the one the catalog hands back. Every
    ///     engine reformats what it stores to some degree, so the comparison is whitespace- and
    ///     case-insensitive, the same way <c>ViewBase</c>'s implementations compare a view.
    /// </summary>
    public static string NormalizeBody(string body)
        => body.Replace("\r\n", "")
            .Replace("\n", "")
            .Replace("\r", "")
            .Replace("\t", "")
            .Replace(" ", "")
            .Trim()
            .TrimEnd(';')
            .ToUpperInvariant();
}
