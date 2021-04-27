using System;
using System.IO;

namespace Weasel.Postgresql
{
    [Obsolete("Doesn't add any value")]
    public interface IDDLRunner
    {
        void Apply(object subject, string ddl);
    }

    public class DDLRecorder: IDDLRunner
    {
        public DDLRecorder() : this(new StringWriter())
        {
        }

        public DDLRecorder(StringWriter writer)
        {
            Writer = writer;
        }

        public StringWriter Writer { get; }

        public void Apply(object subject, string ddl)
        {
            Writer.WriteLine($"-- {subject}");
            Writer.WriteLine(ddl);
            Writer.WriteLine("");
            Writer.WriteLine("");
        }
    }
}
