# Weasel

[![Nuget Package](https://badgen.net/nuget/v/weasel.core)](https://www.nuget.org/packages/Weasel.Core/)
[![Nuget](https://img.shields.io/nuget/dt/weasel.core)](https://www.nuget.org/packages/Weasel.Core/)

Weasel is a library for low level database development with Postgresql (for now)
and Sql Server (later). Weasel is in the process of being extracted from [Marten](https://martendb.io)
with the goal of making this code reusable in other projects.

## Running tests locally

To run tests, you need to set up databases locally. The easiest option is to do it by running Docker images. You can use [predefined Docker Compose setup](./docker-compose.yml) by calling in your terminal:

```bash
docker compose up
```

It'll spin up PostgreSQL and MSSQL databases.

If you'd like to run different database versions, you can do it by setting `POSTGRES_IMAGE` or `MSSQL_IMAGE` env variables, for instance:

```bash
POSTGRES_IMAGE=postgres:15.3-alpine MSSQL_IMAGE=mcr.microsoft.com/mssql/server:2022-latest docker compose up
```

Then, you can run tests from the terminal:

```bash
dotnet test
```

Or your favourite IDE.

Some of our tests are run against a particular PostgreSQL version. Tests explorer should be able to detect database version automatically, but if it's not able to do it, you can enforce it by setting `postgresql_version` to a specific one (e.g.)

```shell
postgresql_version=15.3
```

By default Postgres tests are run with case insensitive names. To run tests against case sensitive, set environment variable:

```
USE_CASE_SENSITIVE_QUALIFIED_NAMES=true
```
