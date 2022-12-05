namespace Weasel.Postgresql.SqlGeneration;

public interface IReversibleWhereFragment: ISqlFragment
{
    /// <summary>
    ///     Effectively create a "reversed" NOT where fragment
    /// </summary>
    /// <returns></returns>
    ISqlFragment Reverse();
}
