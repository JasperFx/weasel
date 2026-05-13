namespace Weasel.Core;

/// <summary>
///     Thrown when a foreign key definition is incomplete or inconsistent
///     (for example, mismatched column counts on the dependent and principal
///     sides). Previously defined separately by each provider's
///     <c>ForeignKey</c> file; in 9.0 the canonical type lives here and the
///     provider-specific duplicates were removed.
/// </summary>
public class MisconfiguredForeignKeyException: Exception
{
    public MisconfiguredForeignKeyException(string? message): base(message)
    {
    }
}
