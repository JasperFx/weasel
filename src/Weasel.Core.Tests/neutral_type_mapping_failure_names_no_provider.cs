using System;
using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Core.Tests;

/// <summary>
///     weasel#602. <see cref="DbCommandBuilder" /> is the provider-neutral builder in Weasel.Core,
///     and its unmapped-type failure said "Weasel.SqlServer does not (yet) support ..." -- naming
///     the wrong package on every provider but one, and sending a PostgreSQL or SQLite user to
///     look at SQL Server's type mappings.
/// </summary>
public class neutral_type_mapping_failure_names_no_provider
{
    private sealed class Unmapped;

    [Fact]
    public void does_not_name_a_provider_package()
    {
        var ex = Should.Throw<NotSupportedException>(
            () => DbDatabaseProvider.Instance.GetDatabaseType(typeof(Unmapped), EnumStorage.AsInteger));

        ex.Message.ShouldNotContain("Weasel.SqlServer");
        ex.Message.ShouldContain("does not (yet) support database type mapping");
    }
}
