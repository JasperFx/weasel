namespace Weasel.Core
{
    public class SqlTemplate
    {
        private readonly string _name;

        public SqlTemplate(string name)
        {
            _name = name;
        }

        public string? TableCreation { get; set; }
        public string? FunctionCreation { get; set; }
    }
}
