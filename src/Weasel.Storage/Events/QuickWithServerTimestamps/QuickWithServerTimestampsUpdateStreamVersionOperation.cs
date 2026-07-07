#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Closed-shape <see cref="UpdateStreamVersionOperationBase"/> for the
/// QuickWithServerTimestamps path. Symmetric with the Rich + Quick equivalents.
/// Relocated from Marten (event E3).
/// </summary>
internal sealed class QuickWithServerTimestampsUpdateStreamVersionOperation: UpdateStreamVersionOperationBase
{
    private readonly QuickWithServerTimestampsEventStorageDescriptor _descriptor;

    public QuickWithServerTimestampsUpdateStreamVersionOperation(
        QuickWithServerTimestampsEventStorageDescriptor descriptor, StreamAction stream): base(stream)
    {
        _descriptor = descriptor;
    }

    public override void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        _descriptor.ConfigureUpdateStreamVersionCommand(builder, Stream);
    }
}
