namespace Weasel.Postgresql.Tables
{
    public enum IndexMethod
    {
        btree,
        hash,
        gist,
        gin,
        brin,
        custom
    }
}