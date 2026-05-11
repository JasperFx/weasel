namespace Weasel.Core;

/// <summary>
/// Strategy for how a bulk-insert operation reconciles incoming rows with rows
/// already present in the target table.
///
/// Promoted to Weasel.Core in 9.0 from Marten and Polecat per the
/// Critter Stack 2026 dedup audit
/// (<see href="https://github.com/JasperFx/jasperfx/issues/214">pillar #214</see>
/// Rule 4 + Rule 2; audit row
/// <see href="https://github.com/JasperFx/weasel/issues/264">weasel#264</see>).
///
/// Marten's pre-9.0 enum (4 values) is the canonical shape. Polecat's pre-4.0
/// enum had 3 of the 4 modes; the missing <see cref="OverwriteIfVersionMatches"/>
/// is implemented on the Polecat bulk-insert path as part of the same audit row.
/// </summary>
public enum BulkInsertMode
{
    /// <summary>
    /// Default, fast mode. Throws if any incoming row's id collides with an
    /// existing row in the target table.
    /// </summary>
    InsertsOnly,

    /// <summary>
    /// Ignores any incoming rows whose ids already exist in the target table.
    /// Existing rows are left untouched.
    /// </summary>
    IgnoreDuplicates,

    /// <summary>
    /// Overwrites any existing rows that collide with incoming ids
    /// (last-write-wins).
    /// </summary>
    OverwriteExisting,

    /// <summary>
    /// Overwrites only when the expected version on the incoming row matches
    /// the stored version of the existing row. Requires the target table to
    /// have a version column wired through the consuming product's schema
    /// (Marten's metadata or Polecat's equivalent).
    /// </summary>
    OverwriteIfVersionMatches
}
