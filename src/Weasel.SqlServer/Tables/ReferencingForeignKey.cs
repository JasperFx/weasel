using Weasel.Core;

namespace Weasel.SqlServer.Tables;

/// <summary>
///     A foreign key on another table that references the table it was read with: <see cref="Table" /> is
///     the table that holds the constraint, and <see cref="ForeignKey" />'s <c>LinkedTable</c> is the one
///     it references.
/// </summary>
public sealed record ReferencingForeignKey(DbObjectName Table, ForeignKey ForeignKey);
