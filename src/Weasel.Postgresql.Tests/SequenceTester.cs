using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests;

[Collection("sequences")]
public class SequenceTester: IntegrationContext
{
    private readonly Sequence theSequence = new(PostgresqlProvider.ToDbObjectName("sequences", "mysequence"));//mySeQuEnCe

    public SequenceTester(): base("sequences")
    {
    }

    [InlineData("seq1")]
    [InlineData("seq1UpErCaSe")]
    [Theory]
    public async Task can_create_sequence_without_blowing_up(string sequenceName)
    {
        var sequence = new Sequence(PostgresqlProvider.ToDbObjectName("sequences", sequenceName));

        await ResetSchema();

        await sequence.CreateAsync(theConnection);
    }

    [InlineData("seq1")]
    [InlineData("seq1UpErCaSe")]
    [Theory]
    public async Task can_create_with_startup_sequence_without_blowing_up(string sequenceName)
    {
        var sequence = new Sequence(PostgresqlProvider.ToDbObjectName("sequences", sequenceName), 5);

        await ResetSchema();

        await sequence.CreateAsync(theConnection);
    }

    [Fact]
    public async Task determine_that_it_is_missing()
    {
        await ResetSchema();

        var delta = await theSequence.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }

    [InlineData("seq1", false)]
    [InlineData("seq1UpErCaSe", true)]
    [Theory]
    public async Task determine_that_it_is_already_there(string sequenceName, bool isCaseSensitive)
    {
        if (PostgresqlProvider.Instance.UseCaseSensitiveQualifiedNames != isCaseSensitive)
            return;

        await can_create_sequence_without_blowing_up(sequenceName);

        var sequence = new Sequence(PostgresqlProvider.ToDbObjectName("sequences", sequenceName));

        var delta = await sequence.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);

    }

    [InlineData("seq1", false)]
    [InlineData("seq1UpErCaSe", true)]
    [Theory]
    public async Task determine_that_it_is_already_there_when_contain_uppercase(string sequenceName, bool isCaseSensitive)
    {
        if (PostgresqlProvider.Instance.UseCaseSensitiveQualifiedNames != isCaseSensitive)
            return;

        await can_create_sequence_without_blowing_up(sequenceName);

        var sequence = new Sequence(PostgresqlProvider.ToDbObjectName("sequences", sequenceName));

        var delta = await sequence.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.None);
    }

    [Fact]
    public async Task create_and_drop()
    {
        await ResetSchema();

        await theSequence.CreateAsync(theConnection);

        var delta = await theSequence.FindDeltaAsync(theConnection);
        delta.Difference.ShouldBe(SchemaPatchDifference.None);

        await theSequence.DropAsync(theConnection);

        delta = await theSequence.FindDeltaAsync(theConnection);

        delta.Difference.ShouldBe(SchemaPatchDifference.Create);
    }
}
