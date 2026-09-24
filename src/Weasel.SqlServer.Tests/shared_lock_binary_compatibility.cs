using System.Reflection;
using Microsoft.Data.SqlClient;
using Weasel.Core.Migrations;
using Shouldly;
using Xunit;

namespace Weasel.SqlServer.Tests;

/// <summary>
///     weasel#616: 9.33.0 added a defaulted <c>lockTimeoutMs</c> to four public
///     <see cref="SharedLockExtensions" /> methods, which removed the signatures every
///     already-compiled consumer binds to. Adding an optional parameter is source compatible and
///     binary breaking, and nothing in the normal pipeline notices -- restore is clean because the
///     new version satisfies the old floor, compile is clean because the caller is pre-compiled,
///     and the <see cref="MissingMethodException" /> only arrives when a host boots and takes the
///     lock.
///     <para>
///         So these are reflection assertions rather than behavioural ones: the thing that broke
///         was the shape of the assembly, and the shape is what has to be pinned. Deleting a case
///         here is a decision to break every consumer compiled against that signature.
///     </para>
/// </summary>
public class shared_lock_binary_compatibility
{
    public static TheoryData<string, Type[], Type> ShippedSignatures => new()
    {
        // shipped through 9.32.0, removed by 9.33.0, restored as forwarders
        { nameof(SharedLockExtensions.GetGlobalTxLock), [typeof(SqlTransaction), typeof(string), typeof(CancellationToken)], typeof(Task) },
        { nameof(SharedLockExtensions.TryGetGlobalTxLock), [typeof(SqlTransaction), typeof(string), typeof(CancellationToken)], typeof(Task<bool>) },
        { nameof(SharedLockExtensions.GetGlobalLock), [typeof(SqlConnection), typeof(string), typeof(CancellationToken), typeof(SqlTransaction)], typeof(Task) },
        { nameof(SharedLockExtensions.TryGetGlobalLock), [typeof(SqlConnection), typeof(string), typeof(CancellationToken)], typeof(Task<bool>) },

        // unchanged across the release, pinned so it stays that way
        { nameof(SharedLockExtensions.ReleaseGlobalLock), [typeof(SqlConnection), typeof(string), typeof(CancellationToken), typeof(SqlTransaction)], typeof(Task) },

        // the 9.33.0 forms, so the forwarders cannot be "fixed" by deleting the timeout instead
        { nameof(SharedLockExtensions.GetGlobalTxLock), [typeof(SqlTransaction), typeof(string), typeof(CancellationToken), typeof(int?)], typeof(Task) },
        { nameof(SharedLockExtensions.TryGetGlobalTxLock), [typeof(SqlTransaction), typeof(string), typeof(CancellationToken), typeof(int?)], typeof(Task<bool>) },
        { nameof(SharedLockExtensions.GetGlobalLock), [typeof(SqlConnection), typeof(string), typeof(CancellationToken), typeof(SqlTransaction), typeof(int?)], typeof(Task) },
        { nameof(SharedLockExtensions.TryGetGlobalLock), [typeof(SqlConnection), typeof(string), typeof(CancellationToken), typeof(int?)], typeof(Task<bool>) },
        { nameof(SharedLockExtensions.TryAttainGlobalLock), [typeof(SqlConnection), typeof(string), typeof(CancellationToken), typeof(int?)], typeof(Task<AttainLockResult>) }
    };

    [Theory]
    [MemberData(nameof(ShippedSignatures))]
    public void the_signature_is_still_on_the_assembly(string name, Type[] parameters, Type returnType)
    {
        var method = typeof(SharedLockExtensions).GetMethod(
            name, BindingFlags.Public | BindingFlags.Static, binder: null, types: parameters, modifiers: null);

        method.ShouldNotBeNull(
            $"{name}({string.Join(", ", parameters.Select(x => x.Name))}) is gone from the assembly. "
            + "Every consumer compiled against it gets MissingMethodException at runtime -- see weasel#616.");

        // the runtime keys on the return type too, so narrowing or widening one is the same break
        method!.ReturnType.ShouldBe(returnType);
    }

    /// <summary>
    ///     The forwarders only work if a three-argument call still resolves. If the overloads were
    ///     ambiguous this would not compile, which is the point of writing it out longhand.
    /// </summary>
    [Fact]
    public void the_pre_9_33_call_shapes_still_bind_at_the_source_level()
    {
        Should.NotThrow(() =>
        {
            Func<SqlConnection, Task<bool>> tryGet = c => c.TryGetGlobalLock("1", CancellationToken.None);
            Func<SqlConnection, Task> get = c => c.GetGlobalLock("1", CancellationToken.None, null);
            Func<SqlTransaction, Task<bool>> tryGetTx = t => t.TryGetGlobalTxLock("1", CancellationToken.None);
            Func<SqlTransaction, Task> getTx = t => t.GetGlobalTxLock("1", CancellationToken.None);

            tryGet.ShouldNotBeNull();
            get.ShouldNotBeNull();
            tryGetTx.ShouldNotBeNull();
            getTx.ShouldNotBeNull();
        });
    }
}
