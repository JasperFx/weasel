import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'

export default withMermaid(
  defineConfig({
    title: 'Weasel',
    description: 'Database schema management and migration for .NET',
    head: [
      ['link', { rel: 'icon', href: '/jasperfx-logo.png' }]
    ],

    themeConfig: {
      logo: '/jasperfx-logo.png',

      nav: [
        { text: 'Guide', link: '/guide/' },
        { text: 'CLI Tools', link: '/cli/' },
        {
          text: 'Databases',
          items: [
            { text: 'PostgreSQL', link: '/postgresql/' },
            { text: 'SQL Server', link: '/sqlserver/' },
            { text: 'Oracle', link: '/oracle/' },
            { text: 'MySQL', link: '/mysql/' },
            { text: 'SQLite', link: '/sqlite/' }
          ]
        },
        { text: 'EF Core', link: '/efcore/' },
        {
          text: 'Resources',
          items: [
            { text: 'NuGet Packages', link: 'https://www.nuget.org/profiles/jeremydmiller' },
            { text: 'GitHub', link: 'https://github.com/JasperFx/weasel' },
            { text: 'JasperFx Software', link: 'https://jasperfx.net' }
          ]
        }
      ],

      sidebar: [
        {
          text: 'Getting Started',
          collapsed: false,
          items: [
            { text: 'Introduction', link: '/guide/' },
            { text: 'Installation', link: '/guide/installation' },
            { text: 'Quick Start', link: '/guide/quickstart' }
          ]
        },
        {
          text: 'Core Concepts',
          collapsed: false,
          items: [
            { text: 'Schema Objects', link: '/core/schema-objects' },
            { text: 'Schema Migrations', link: '/core/schema-migrations' },
            { text: 'Command Builders & Batching', link: '/core/command-builders' },
            { text: 'Extension Methods', link: '/core/extension-methods' },
            { text: 'Multi-Tenancy', link: '/core/multi-tenancy' }
          ]
        },
        {
          text: 'Command Line Tools',
          collapsed: false,
          items: [
            { text: 'Setup & Integration', link: '/cli/' },
            { text: 'db-apply', link: '/cli/db-apply' },
            { text: 'db-assert', link: '/cli/db-assert' },
            { text: 'db-patch', link: '/cli/db-patch' },
            { text: 'db-dump', link: '/cli/db-dump' },
            { text: 'db-list', link: '/cli/db-list' }
          ]
        },
        {
          text: 'PostgreSQL',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/postgresql/' },
            { text: 'Tables', link: '/postgresql/tables' },
            { text: 'Partitioning', link: '/postgresql/partitioning' },
            { text: 'Functions', link: '/postgresql/functions' },
            { text: 'Sequences', link: '/postgresql/sequences' },
            { text: 'Views', link: '/postgresql/views' },
            { text: 'Extensions', link: '/postgresql/extensions' },
            { text: 'NetTopologySuite', link: '/postgresql/nts' }
          ]
        },
        {
          text: 'SQL Server',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/sqlserver/' },
            { text: 'Tables', link: '/sqlserver/tables' },
            { text: 'Stored Procedures', link: '/sqlserver/procedures' },
            { text: 'Functions', link: '/sqlserver/functions' },
            { text: 'Sequences', link: '/sqlserver/sequences' },
            { text: 'Table Types', link: '/sqlserver/table-types' }
          ]
        },
        {
          text: 'Oracle',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/oracle/' },
            { text: 'Tables', link: '/oracle/tables' },
            { text: 'Sequences', link: '/oracle/sequences' }
          ]
        },
        {
          text: 'MySQL',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/mysql/' },
            { text: 'Tables', link: '/mysql/tables' },
            { text: 'Sequences', link: '/mysql/sequences' }
          ]
        },
        {
          text: 'SQLite',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/sqlite/' },
            { text: 'Tables', link: '/sqlite/tables' },
            { text: 'Views', link: '/sqlite/views' },
            { text: 'JSON Support', link: '/sqlite/json' },
            { text: 'PRAGMA Settings', link: '/sqlite/pragmas' },
            { text: 'SqliteHelper', link: '/sqlite/helper' }
          ]
        },
        {
          text: 'EF Core Integration',
          collapsed: true,
          items: [
            { text: 'Overview', link: '/efcore/' },
            { text: 'Table Mapping', link: '/efcore/table-mapping' },
            { text: 'Migrations', link: '/efcore/migrations' },
            { text: 'JSON Columns', link: '/efcore/json-columns' },
            { text: 'Database Reset for Testing', link: '/efcore/database-cleaner' },
            { text: 'Batch Queries', link: '/efcore/batch-queries' }
          ]
        }
      ],

      socialLinks: [
        { icon: 'github', link: 'https://github.com/JasperFx/weasel' }
      ],

      editLink: {
        pattern: 'https://github.com/JasperFx/weasel/edit/master/docs/:path'
      },

      footer: {
        message: 'Released under the MIT License.',
        copyright: 'Copyright JasperFx Software'
      },

      search: {
        provider: 'local'
      }
    },

    mermaid: {},
    mermaidPlugin: {
      class: 'mermaid'
    }
  })
)
