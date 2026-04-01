---
layout: home

hero:
  name: Weasel
  text: Database Schema Management for .NET
  tagline: Programmatic schema definitions, automatic migrations with delta detection, and ADO.NET helpers across PostgreSQL, SQL Server, Oracle, MySQL, and SQLite.
  image:
    src: /jasperfx-logo.png
    alt: Weasel
  actions:
    - theme: brand
      text: Get Started
      link: /guide/
    - theme: alt
      text: View on GitHub
      link: https://github.com/JasperFx/weasel

features:
  - icon: <img src="/postgresql-logo.svg" alt="PostgreSQL" />
    title: Multi-Database Support
    details: First-class support for PostgreSQL, SQL Server, Oracle, MySQL, and SQLite with provider-specific features like partitioning, stored procedures, and JSON columns.
  - icon: "\U0001F504"
    title: Schema Migrations
    details: Automatic delta detection compares your in-memory schema definitions against the actual database and applies only the necessary changes.
  - icon: "\U0001F4E6"
    title: Command Builders & Batching
    details: Reduce network round trips with batched command execution for PostgreSQL (NpgsqlBatch) and SQL Server (SqlBatch), plus fluent SQL building helpers.
  - icon: "\U0001F6E0\uFE0F"
    title: CLI Tools
    details: Built-in db-apply, db-assert, db-patch, db-dump, and db-list commands integrate into any .NET application via JasperFx command line tooling.
  - icon: "\U0001F517"
    title: EF Core Integration
    details: Map Entity Framework Core DbContext models to Weasel tables for unified schema migration alongside Marten or Polecat document stores.
  - icon: "\U0001F3E2"
    title: Part of the Critter Stack
    details: Extracted from Marten and used by Wolverine and Polecat. Battle-tested in production across the JasperFx ecosystem.
---
