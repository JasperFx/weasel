namespace Weasel.SqlServer.Tables
{
    public enum IndexMethod
    {
        btree,
        hash,
        gist,
        gin,
        brin
    }
}