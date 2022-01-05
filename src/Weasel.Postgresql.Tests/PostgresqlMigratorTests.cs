using System;
using Shouldly;
using Xunit;

namespace Weasel.Postgresql.Tests
{
    public class PostgresqlMigratorTests
    {
        [Fact]
        public void default_name_data_length_is_64()
        {
            new PostgresqlMigrator().NameDataLength.ShouldBe(64);
        }

        [Fact]
        public void assert_identifier_length_happy_path()
        {
            var options = new PostgresqlMigrator();

            for (var i = 1; i < options.NameDataLength; i++)
            {
                var text = new string('a', i);

                options.AssertValidIdentifier(text);
            }
        }

        [Fact]
        public void assert_identifier_must_not_contain_space()
        {
            var random = new Random();
            var options = new PostgresqlMigrator();

            for (var i = 1; i < options.NameDataLength; i++)
            {
                var text = new string('a', i);
                var position = random.Next(0, i);

                Should.Throw<PostgresqlIdentifierInvalidException>(() =>
                {
                    options.AssertValidIdentifier(text.Remove(position).Insert(position, " "));
                });
            }
        }

        [Fact]
        public void assert_identifier_null_or_whitespace()
        {
            var options = new PostgresqlMigrator();

            Should.Throw<PostgresqlIdentifierInvalidException>(() =>
            {
                options.AssertValidIdentifier(null);
            });

            for (var i = 0; i < options.NameDataLength; i++)
            {
                var text = new string(' ', i);

                Should.Throw<PostgresqlIdentifierInvalidException>(() =>
                {
                    options.AssertValidIdentifier(text);
                });
            }
        }

        [Fact]
        public void assert_identifier_length_exceeding_maximum()
        {
            var options = new PostgresqlMigrator();

            var text = new string('a', options.NameDataLength);

            Should.Throw<PostgresqlIdentifierTooLongException>(() =>
            {
                options.AssertValidIdentifier(text);
            });
        }


    }
}
