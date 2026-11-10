using System.Data.Common;
using NSubstitute;
using Shouldly;
using Weasel.Storage;
using Xunit;

namespace Weasel.Core.Tests.StorageHelpers;

public class storage_casting_selector
{
    public class Animal;

    public class Dog: Animal;

    [Fact]
    public async Task narrows_what_the_root_selector_materializes()
    {
        var dog = new Dog();
        var inner = Substitute.For<ISelector<Animal>>();
        inner.Resolve(Arg.Any<DbDataReader>()).Returns(dog);
        inner.ResolveAsync(Arg.Any<DbDataReader>(), Arg.Any<CancellationToken>()).Returns(Task.FromResult<Animal>(dog));

        var selector = new StorageCastingSelector<Dog, Animal>(inner);
        var reader = Substitute.For<DbDataReader>();

        selector.Resolve(reader).ShouldBeSameAs(dog);
        (await selector.ResolveAsync(reader, CancellationToken.None)).ShouldBeSameAs(dog);
    }

    [Fact]
    public void a_row_of_another_subclass_is_an_invalid_cast_not_a_silent_null()
    {
        var inner = Substitute.For<ISelector<Animal>>();
        inner.Resolve(Arg.Any<DbDataReader>()).Returns(new Animal());

        var selector = new StorageCastingSelector<Dog, Animal>(inner);

        Should.Throw<InvalidCastException>(() => selector.Resolve(Substitute.For<DbDataReader>()));
    }
}
