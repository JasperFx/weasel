// See https://aka.ms/new-console-template for more information

using CommandLineTarget;
using Npgsql;
using Weasel.Postgresql.Tests;

// using var conn = new NpgsqlConnection(ConnectionSource.ConnectionString);
// Console.WriteLine("STart");
// await conn.OpenAsync();
//
// Console.WriteLine("Opened");

var tables = new DatabaseCollection();

tables.AddTable("one", "one", "one");
tables.AddTable("one", "one", "two");
tables.AddTable("one", "one", "three");

tables.AddTable("two", "one", "two.one2");
tables.AddTable("two", "one", "two.two2");
tables.AddTable("two", "one", "two.three2");

tables.AddTable("three", "one", "one3");
tables.AddTable("three", "one", "two3");
tables.AddTable("three", "one", "three3");
tables.AddTable("three", "one", "four");
tables.AddTable("three", "one", "five");

return await tables.Execute(args);
