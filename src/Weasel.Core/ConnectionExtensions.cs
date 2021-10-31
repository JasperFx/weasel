using System.Data.Common;
using System.Threading.Tasks;

namespace Weasel.Core
{
    public static class ConnectionExtensions
    {
#if NETSTANDARD2_0
        public static Task CloseAsync(this DbConnection conn)
        {
            conn.Close();
            return Task.CompletedTask;
        }
#endif
    }
}
