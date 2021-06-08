using System;
using System.IO;
using Baseline;

namespace Weasel.SqlServer
{
    public class DdlRules
    {
        public static readonly string SCHEMA = "%SCHEMA%";
        public static readonly string TABLENAME = "%TABLENAME%";
        public static readonly string FUNCTION = "%FUNCTION%";
        public static readonly string SIGNATURE = "%SIGNATURE%";
        public static readonly string COLUMNS = "%COLUMNS%";
        public static readonly string NON_ID_COLUMNS = "%NON_ID_COLUMNS%";
        public static readonly string METADATA_COLUMNS = "%METADATA_COLUMNS%";

        public readonly LightweightCache<string, DdlTemplate> Templates
            = new(name => new DdlTemplate(name));

        /// <summary>
        ///     Should all generated DDL files be written with transactional semantics
        ///     so that everything succeeds or everything fails together
        /// </summary>
        public bool IsTransactional { get; set; } = true;

        public DdlFormatting Formatting { get; set; } = DdlFormatting.Pretty;

        /// <summary>
        ///     Alters the syntax used to create tables in DDL
        /// </summary>
        public CreationStyle TableCreation { get; set; } = CreationStyle.DropThenCreate;

        /// <summary>
        ///     Alters the user rights for the upsert functions in DDL
        /// </summary>
        public SecurityRights UpsertRights { get; set; } = SecurityRights.Invoker;

        /// <summary>
        ///     Option to use this database role during DDL scripts
        /// </summary>
        public string Role { get; set; }

        /// <summary>
        ///     Read [name].table and [name].function files from the named directory
        ///     to serve as templates for extra DDL (GRANT's probably)
        /// </summary>
        /// <param name="directory"></param>
        public void ReadTemplates(string directory)
        {
            var system = new FileSystem();

            system.FindFiles(directory, FileSet.Shallow("*.function")).Each(file =>
            {
                var name = Path.GetFileNameWithoutExtension(file).ToLower();

                Templates[name].FunctionCreation = system.ReadStringFromFile(file);
            });

            system.FindFiles(directory, FileSet.Shallow("*.table")).Each(file =>
            {
                var name = Path.GetFileNameWithoutExtension(file).ToLower();

                Templates[name].TableCreation = system.ReadStringFromFile(file);
            });
        }

        /// <summary>
        ///     Read DDL templates from the application base directory
        /// </summary>
        public void ReadTemplates()
        {
            ReadTemplates(AppContext.BaseDirectory);
        }

        /// <summary>
        ///     Write templated SQL to the supplied file name
        /// </summary>
        /// <param name="filename"></param>
        /// <param name="writeStep"></param>
        public void WriteTemplatedFile(string filename, Action<DdlRules, TextWriter> writeStep)
        {
            using (var stream = new FileStream(filename, FileMode.Create))
            {
                var writer = new StreamWriter(stream) {AutoFlush = true};

                WriteScript(writer, writeStep);

                stream.Flush(true);
            }
        }

        /// <summary>
        ///     Write out a templated SQL script with all rules
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="writeStep">A continuation to write the inner SQL</param>
        public void WriteScript(TextWriter writer, Action<DdlRules, TextWriter> writeStep)
        {
            if (IsTransactional)
            {
                writer.WriteLine("DO LANGUAGE plpgsql $tran$");
                writer.WriteLine("BEGIN");
                writer.WriteLine("");
            }

            if (Role.IsNotEmpty())
            {
                writer.WriteLine($"SET ROLE {Role};");
                writer.WriteLine("");
            }

            writeStep(this, writer);

            if (Role.IsNotEmpty())
            {
                writer.WriteLine("RESET ROLE;");
                writer.WriteLine("");
            }

            if (IsTransactional)
            {
                writer.WriteLine("");
                writer.WriteLine("END;");
                writer.WriteLine("$tran$;");
            }
        }
    }

    public class DdlTemplate
    {
        private readonly string _name;

        public DdlTemplate(string name)
        {
            _name = name;
        }

        public string TableCreation { get; set; }
        public string FunctionCreation { get; set; }
    }
}