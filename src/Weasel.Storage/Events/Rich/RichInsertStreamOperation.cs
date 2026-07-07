#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Closed-shape <see cref="InsertStreamOperationBase"/> for the Rich (full-mode)
/// path. The complete command shape lives on
/// <see cref="RichEventStorageDescriptor.ConfigureInsertStreamCommand"/> — a
/// closure the dialect composes once at descriptor-build time based on
/// stream-identity (Guid vs string) and tenancy style. This class is the minimal
/// <see cref="IStorageOperation"/> shell: pull the closure off the descriptor,
/// invoke it. Relocated from Marten (event E3).
/// </summary>
internal sealed class RichInsertStreamOperation: InsertStreamOperationBase
{
    private readonly RichEventStorageDescriptor _descriptor;

    public RichInsertStreamOperation(RichEventStorageDescriptor descriptor, StreamAction stream)
        : base(stream, descriptor.TransformInsertStreamException)
    {
        _descriptor = descriptor;
    }

    public override void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        _descriptor.ConfigureInsertStreamCommand(builder, Stream);
    }
}
