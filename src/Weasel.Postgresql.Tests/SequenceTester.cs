﻿using Shouldly;
using Weasel.Core;
using Xunit;

namespace Weasel.Postgresql.Tests;

[Collection("sequences")]
public class SequenceTester: IntegrationContext
{
    private readonly Sequence theSequence = new(new DbObjectName("sequences", "mysequence"));

    public SequenceTester(): base("sequences")
    {
    }


    [Fact]
    public async Task can_create_sequence_without_blowing_up()
    {
        await ResetSchema();

        await theSequence.CreateAsync(theConnection);
    }


    [Fact]
    public async Task can_create_with_startup_sequence_without_blowing_up()
    {
        var sequence = new Sequence(new DbObjectName("sequences", "seq1"), 5);

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

    [Fact]
    public async Task determine_that_it_is_already_there()
    {
        await can_create_sequence_without_blowing_up();

        var delta = await theSequence.FindDeltaAsync(theConnection);

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
