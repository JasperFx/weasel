namespace Weasel.Core;

/// <summary>
///     Pluggable strategy for the provider-specific DDL syntax decisions that
///     vary between databases. The intent is to let a single shared algorithm in
///     <c>TableBase</c> drive the CREATE / DROP / column-rendering flow, with the
///     strategy supplying the actual SQL tokens.
///     <para>
///     <b>Status: prototype.</b> Step 8 of #270 introduces this interface and wires
///     it through PostgreSQL and SQLite — the two providers at the extremes of
///     feature support. If the shape holds for those two, step 9 (TableBase +
///     canonical ColumnExpression) extends it to the remaining three providers
///     and migrates the full CREATE algorithm here. Until then the surface is
///     deliberately small: it covers the parts of <c>Table.WriteCreateStatement</c>
///     / <c>WriteDropStatement</c> that genuinely differ between PG and SQLite
///     (DROP framing, CREATE header, whether FKs are inlined, auto-increment
///     spelling, statement terminator), not the parts that are identical.
///     </para>
///     <para>
///     <b>Why not just more virtual methods on a base?</b> The audit (see #270)
///     identified that ~85% of <c>Table.WriteCreateStatement</c> is the same
///     algorithm across providers — only the syntax tokens vary. A strategy
///     object is easier to test in isolation, easier to mock for unit tests of
///     the algorithm, and keeps <c>TableBase</c> from accumulating ~12 abstract
///     hooks. The trade-off is one extra indirection per token; given DDL is
///     emitted at migration time and never on a hot path, that's fine.
///     </para>
/// </summary>
public interface IDdlSyntaxStrategy
{
    /// <summary>
    ///     Quote an identifier (column name, constraint name, etc.) per the
    ///     provider's rules. PostgreSQL uses double-quotes, MySQL backticks,
    ///     SQL Server square-brackets, SQLite double-quotes.
    /// </summary>
    string QuoteIdentifier(string name);

    /// <summary>
    ///     Write the full DROP TABLE statement(s) for an existing table. Includes
    ///     the terminator. PostgreSQL appends <c>CASCADE</c> (to drop dependent
    ///     views / foreign keys / sequences atomically); SQLite has no CASCADE
    ///     and simply emits <c>DROP TABLE IF EXISTS …;</c>.
    /// </summary>
    void WriteDropTable(TextWriter writer, DbObjectName identifier);

    /// <summary>
    ///     Write the CREATE TABLE header line up to and including the open
    ///     paren — e.g. <c>CREATE TABLE IF NOT EXISTS "x" (</c>. Caller writes
    ///     the column body and the close.
    /// </summary>
    /// <param name="style">
    ///     When <see cref="CreationStyle.DropThenCreate" />, the caller has
    ///     already emitted a DROP via <see cref="WriteDropTable" /> and the header
    ///     should omit <c>IF NOT EXISTS</c>. When
    ///     <see cref="CreationStyle.CreateIfNotExists" />, the header should
    ///     include the guard.
    /// </param>
    void WriteCreateTableHeader(TextWriter writer, DbObjectName identifier, CreationStyle style);

    /// <summary>
    ///     True when foreign-key <c>CONSTRAINT … FOREIGN KEY (…) REFERENCES …</c>
    ///     clauses must be emitted inline inside the CREATE TABLE body (SQLite —
    ///     it has no <c>ALTER TABLE ADD CONSTRAINT</c> for FKs). False when FKs
    ///     are emitted as separate <c>ALTER TABLE ADD CONSTRAINT</c> statements
    ///     after the CREATE (PostgreSQL, SQL Server, Oracle, MySQL).
    /// </summary>
    bool InlineForeignKeyConstraints { get; }

    /// <summary>
    ///     Token spelling for column-level auto-increment / identity: PostgreSQL
    ///     <c>SERIAL</c> / <c>BIGSERIAL</c>, SQLite <c>AUTOINCREMENT</c>, SQL
    ///     Server / Oracle <c>IDENTITY(1,1)</c>, MySQL <c>AUTO_INCREMENT</c>.
    ///     Surfaced here so a unified <c>ColumnExpression.AutoIncrement()</c>
    ///     fluent call in <c>Weasel.Core</c> can resolve the right SQL token
    ///     via the active strategy (step 10 of #270 standardises the fluent
    ///     naming on <c>AutoIncrement</c> and keeps PG's <c>Serial()</c> as an
    ///     <c>[Obsolete]</c> alias).
    /// </summary>
    string AutoIncrementToken { get; }

    /// <summary>
    ///     Per-statement terminator. <c>";"</c> on every provider except Oracle,
    ///     where a PL/SQL-style <c>"/"</c> on its own line terminates each
    ///     statement in a script. Used by the batched DDL writers.
    /// </summary>
    string StatementTerminator { get; }
}
