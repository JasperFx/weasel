#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Closed-shape <see cref="UpdateStreamVersionOperationBase"/> for the Rich
/// (full-mode) path. Like <see cref="RichInsertStreamOperation"/> the command
/// shape lives on a descriptor-owned closure
/// (<see cref="RichEventStorageDescriptor.ConfigureUpdateStreamVersionCommand"/>).
/// The base's <c>PostprocessAsync</c> raises the expected-version-mismatch
/// exception when no row was updated. Relocated from Marten (event E3).
/// </summary>
internal sealed class RichUpdateStreamVersionOperation: UpdateStreamVersionOperationBase
{
    private readonly RichEventStorageDescriptor _descriptor;

    public RichUpdateStreamVersionOperation(RichEventStorageDescriptor descriptor, StreamAction stream): base(stream)
    {
        _descriptor = descriptor;
    }

    public override void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        _descriptor.ConfigureUpdateStreamVersionCommand(builder, Stream);
    }
}
