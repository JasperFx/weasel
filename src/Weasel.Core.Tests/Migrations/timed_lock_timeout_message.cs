using System;
using System.Threading.Tasks;
using JasperFx.Core;
using Shouldly;
using Weasel.Core.Migrations;
using Xunit;

namespace Weasel.Core.Tests.Migrations;

/// <summary>
///     weasel#602. The in-process migration lock threw a bare <see cref="TimeoutException" /> with
///     no message at all -- "The operation has timed out." and nothing about what had timed out,
///     what was holding it, or what to do instead.
/// </summary>
public class timed_lock_timeout_message
{
    [Fact]
    public async Task says_what_timed_out_and_what_to_do_about_it()
    {
        var theLock = new TimedLock();

        using var _ = await theLock.Lock(1.Seconds(), "applying the schema for Documents");

        var ex = await Should.ThrowAsync<TimeoutException>(
            () => theLock.Lock(50.Milliseconds(), "applying the schema for Documents"));

        ex.Message.ShouldContain("Timed out after 0.05s");
        ex.Message.ShouldContain("another thread in this process");
        ex.Message.ShouldContain("applying the schema for Documents");

        // The remedy, which is the point: a runtime storage check only contends like this because
        // migrations were not applied up front.
        ex.Message.ShouldContain("ApplyAllConfiguredChangesToDatabaseAsync");
        ex.Message.ShouldContain("db-apply");
    }

    [Fact]
    public async Task still_reads_when_the_caller_names_nothing()
    {
        var theLock = new TimedLock();

        using var _ = await theLock.Lock(1.Seconds());

        (await Should.ThrowAsync<TimeoutException>(() => theLock.Lock(50.Milliseconds())))
            .Message.ShouldContain("finish applying the schema.");
    }
}
