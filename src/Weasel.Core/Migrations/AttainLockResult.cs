namespace Weasel.Core.Migrations;

public record AttainLockResult(bool Succeeded, AttainLockResult.FailureReason Reason)
{
    public bool ShouldReconnect => Reason == FailureReason.DatabaseNotAvailable;

    public static readonly AttainLockResult Success = new(true, FailureReason.None);

    public static AttainLockResult Failure(FailureReason reason = FailureReason.Failure) => new(false, reason);

    public enum FailureReason
    {
        None,
        Failure,
        DatabaseNotAvailable
    }
}
