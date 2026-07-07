#nullable enable
using JasperFx.Events;
using Weasel.Core;

namespace Weasel.Storage;

/// <summary>
/// Closed-shape <see cref="InsertStreamOperationBase"/> for the
/// QuickWithServerTimestamps path. Symmetric with the Rich + Quick equivalents —
/// delegates to a descriptor-installed closure. Relocated from Marten (event E3).
/// </summary>
internal sealed class QuickWithServerTimestampsInsertStreamOperation: InsertStreamOperationBase
{
    private readonly QuickWithServerTimestampsEventStorageDescriptor _descriptor;

    public QuickWithServerTimestampsInsertStreamOperation(
        QuickWithServerTimestampsEventStorageDescriptor descriptor, StreamAction stream)
        : base(stream, descriptor.TransformInsertStreamException)
    {
        _descriptor = descriptor;
    }

    public override void ConfigureCommand(ICommandBuilder builder, IStorageSession session)
    {
        _descriptor.ConfigureInsertStreamCommand(builder, Stream);
    }
}
