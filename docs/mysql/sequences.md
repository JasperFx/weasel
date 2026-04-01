# Sequences

The `Sequence` class in `Weasel.MySql` provides sequence-like behavior using a table-based implementation for broad MySQL version compatibility.

## How It Works

Since native `CREATE SEQUENCE` support was only added in MySQL 8.0 (via MariaDB compatibility), Weasel.MySql implements sequences as tables with an AUTO_INCREMENT primary key and a `current_value` column. This works across all MySQL versions.

## Defining a Sequence

<!-- snippet: sample_mysql_define_sequence -->
<a id='snippet-sample_mysql_define_sequence'></a>
```cs
var seq = new Sequence("order_seq");
seq.StartWith = 1000;
seq.IncrementBy = 1;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L104-L108' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_define_sequence' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Generating DDL

<!-- snippet: sample_mysql_sequence_create_ddl -->
<a id='snippet-sample_mysql_sequence_create_ddl'></a>
```cs
var migrator = new MySqlMigrator();
var writer = new StringWriter();
seq.WriteCreateStatement(migrator, writer);
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L115-L119' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_sequence_create_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

This generates:

```sql
CREATE TABLE IF NOT EXISTS `public`.`order_seq` (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    current_value BIGINT NOT NULL DEFAULT 1000
);
INSERT IGNORE INTO `public`.`order_seq` (current_value) VALUES (1000);
```

To drop:

<!-- snippet: sample_mysql_sequence_drop_ddl -->
<a id='snippet-sample_mysql_sequence_drop_ddl'></a>
```cs
seq.WriteDropStatement(migrator, writer);
// Output: DROP TABLE IF EXISTS `public`.`order_seq`;
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L128-L131' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_sequence_drop_ddl' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Delta Detection

Sequence existence is checked via `information_schema.tables`:

<!-- snippet: sample_mysql_sequence_delta_detection -->
<a id='snippet-sample_mysql_sequence_delta_detection'></a>
```cs
// Delta detection happens automatically during schema migration.
// The sequence is created if the backing table does not exist.
```
<sup><a href='https://github.com/JasperFx/weasel/blob/master/src/DocSamples/MySqlSamples.cs#L136-L139' title='Snippet source file'>snippet source</a> | <a href='#snippet-sample_mysql_sequence_delta_detection' title='Start of snippet'>anchor</a></sup>
<!-- endSnippet -->

## Usage Pattern

To get the next value from the sequence in application code:

```sql
UPDATE `order_seq` SET current_value = LAST_INSERT_ID(current_value + 1);
SELECT LAST_INSERT_ID();
```
