#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Closed-shape <see cref="InsertStreamOperationBase"/> for the Quick-mode paths.
/// Identical shape to <see cref="RichInsertStreamOperation"/> — delegates to a
/// descriptor-installed closure built by the dialect. Relocated from Marten
/// (event E3).
/// </summary>
internal sealed class QuickInsertStreamOperation: InsertStreamOperationBase
{
    private readonly QuickEventStorageDescriptor _descriptor;

    public QuickInsertStreamOperation(QuickEventStorageDescriptor descriptor, StreamAction stream)
        : base(stream, descriptor.TransformInsertStreamException)
    {
        _descriptor = descriptor;
    }

    public override void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        _descriptor.ConfigureInsertStreamCommand(builder, Stream);
    }
}
