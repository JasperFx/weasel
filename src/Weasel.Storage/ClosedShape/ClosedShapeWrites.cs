#nullable enable
using Weasel.Core;

namespace Weasel.Storage;

// The write-operation factories, one set per concurrency mode. Every tracking mode builds the same
// operations; only the selector differs. The session paths pass the session's version dictionary for
// the guard, the projection paths pass none, so parallel daemon workers sharing one session never
// touch it. The descriptor each write runs over comes from the rules object's WriteDescriptorFor.

internal static class UnversionedClosedShapeWrites<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public static IStorageOperation Insert(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeInsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Insert, document, session));

    public static IStorageOperation Update(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeUpdateOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Update, document, session));

    public static IStorageOperation Upsert(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeUpsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Upsert, document, session), OperationRole.Upsert);

    public static IStorageOperation Overwrite(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new UnversionedClosedShapeOverwriteOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Other, document, session));

    public static IStorageOperation InsertProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new UnversionedClosedShapeInsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Insert, document, null));

    public static IStorageOperation UpdateProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new UnversionedClosedShapeUpdateOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Update, document, null));

    public static IStorageOperation UpsertProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new UnversionedClosedShapeUpsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Upsert, document, null), OperationRole.Upsert);

    public static IStorageOperation OverwriteProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new UnversionedClosedShapeOverwriteOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Other, document, null));
}

internal static class OptimisticClosedShapeWrites<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public static IStorageOperation Insert(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeInsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Insert, document, session), session.Versions.ForType<TDoc, TId>());

    public static IStorageOperation Update(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeUpdateOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Update, document, session), session.Versions.ForType<TDoc, TId>());

    public static IStorageOperation Upsert(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeUpsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Upsert, document, session), OperationRole.Upsert, session.Versions.ForType<TDoc, TId>());

    public static IStorageOperation Overwrite(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new OptimisticClosedShapeOverwriteOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Other, document, session), session.Versions.ForType<TDoc, TId>());

    public static IStorageOperation InsertProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new OptimisticClosedShapeInsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Insert, document, null), null);

    public static IStorageOperation UpdateProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new OptimisticClosedShapeUpdateOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Update, document, null), null);

    public static IStorageOperation UpsertProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new OptimisticClosedShapeUpsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Upsert, document, null), OperationRole.Upsert, null);

    public static IStorageOperation OverwriteProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new OptimisticClosedShapeOverwriteOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Other, document, null), null);
}

internal static class NumericClosedShapeWrites<TDoc, TId>
    where TDoc : notnull
    where TId : notnull
{
    public static IStorageOperation Insert(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeInsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Insert, document, session), session.Versions.RevisionsFor<TDoc, TId>());

    public static IStorageOperation Update(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeUpdateOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Update, document, session), session.Versions.RevisionsFor<TDoc, TId>());

    public static IStorageOperation Upsert(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeUpsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Upsert, document, session), OperationRole.Upsert, session.Versions.RevisionsFor<TDoc, TId>());

    public static IStorageOperation Overwrite(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, IStorageSession session, string tenantId)
        => new NumericClosedShapeOverwriteOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Other, document, session), session.Versions.RevisionsFor<TDoc, TId>());

    public static IStorageOperation InsertProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new NumericClosedShapeInsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Insert, document, null), null);

    public static IStorageOperation UpdateProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new NumericClosedShapeUpdateOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Update, document, null), null);

    public static IStorageOperation UpsertProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new NumericClosedShapeUpsertOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Upsert, document, null), OperationRole.Upsert, null);

    public static IStorageOperation OverwriteProjected(ClosedShapeDocumentStorage<TDoc, TId> s, TDoc document, string tenantId)
        => new NumericClosedShapeOverwriteOperation<TDoc, TId>(document, s.Identity(document), tenantId, s.WriteDescriptor(OperationRole.Other, document, null), null);
}
