using Shouldly;
using Weasel.Core;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

using IStorageOperation = Weasel.Storage.IStorageOperation;

public class storage_change_set
{
    public class Doc;

    [Fact]
    public void documents_are_partitioned_by_role()
    {
        var inserted = new Doc();
        var updated = new Doc();
        var upserted = new Doc();

        var changes = new StorageChangeSet([
            new FakeDocumentOperation(inserted, OperationRole.Insert),
            new FakeDocumentOperation(updated, OperationRole.Update),
            new FakeDocumentOperation(upserted, OperationRole.Upsert),
            new FakeOperation(OperationRole.Other)
        ]);

        changes.Inserted.ShouldBe([inserted]);
        changes.Updated.ShouldBe([updated, upserted]);
        changes.Deleted.ShouldBeEmpty();
    }

    [Fact]
    public void a_by_id_deletion_reports_its_id_even_when_its_role_is_not_deletion()
    {
        // A soft delete is an UPDATE. Classifying by role first would lose the id.
        var changes = new StorageChangeSet([
            new FakeDeletion(typeof(Doc), 42),
            new FakeDeletion(typeof(Doc), 43, OperationRole.Update)
        ]);

        changes.Deleted.Select(x => x.Id).ShouldBe([42, 43]);
        changes.Deleted.ShouldAllBe(x => x.DocumentType == typeof(Doc));
        changes.Updated.ShouldBeEmpty();
    }

    [Fact]
    public void a_predicate_deletion_is_reported_with_no_id()
    {
        var changes = new StorageChangeSet([new FakeOperation(OperationRole.Deletion, typeof(Doc))]);

        var deletion = changes.Deleted.ShouldHaveSingleItem();
        deletion.DocumentType.ShouldBe(typeof(Doc));
        deletion.Id.ShouldBeNull();
    }

    [Fact]
    public void the_hooks_surface_what_a_store_s_own_operations_carry()
    {
        var bespoke = new Doc();
        var insert = new FakeOperation(OperationRole.Insert, typeof(Doc));
        var delete = new FakeOperation(OperationRole.Deletion, typeof(Doc));

        var changes = new StorageChangeSet([insert, delete],
            op => ReferenceEquals(op, insert) ? bespoke : null,
            op => ReferenceEquals(op, delete) ? 7 : null);

        changes.Inserted.ShouldBe([bespoke]);
        changes.Deleted.ShouldHaveSingleItem().Id.ShouldBe(7);
    }

    [Fact]
    public void an_operation_with_no_document_is_left_out()
    {
        var changes = new StorageChangeSet([new FakeOperation(OperationRole.Upsert, typeof(Doc))]);

        changes.Updated.ShouldBeEmpty();
    }

    [Fact]
    public void it_is_a_snapshot_of_the_list_it_was_built_from()
    {
        var operations = new List<IStorageOperation> { new FakeDocumentOperation(new Doc(), OperationRole.Insert) };
        var changes = new StorageChangeSet(operations);

        operations.Add(new FakeDocumentOperation(new Doc(), OperationRole.Insert));
        operations.Clear();

        changes.Inserted.Count.ShouldBe(1);
    }
}
