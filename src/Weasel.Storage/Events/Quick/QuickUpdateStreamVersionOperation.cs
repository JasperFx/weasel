#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Closed-shape <see cref="UpdateStreamVersionOperationBase"/> for the Quick-mode
/// paths. Symmetric with <see cref="RichUpdateStreamVersionOperation"/>. Relocated
/// from Marten (event E3).
/// </summary>
internal sealed class QuickUpdateStreamVersionOperation: UpdateStreamVersionOperationBase
{
    private readonly QuickEventStorageDescriptor _descriptor;

    public QuickUpdateStreamVersionOperation(QuickEventStorageDescriptor descriptor, StreamAction stream): base(stream)
    {
        _descriptor = descriptor;
    }

    public override void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        _descriptor.ConfigureUpdateStreamVersionCommand(builder, Stream);
    }
}
