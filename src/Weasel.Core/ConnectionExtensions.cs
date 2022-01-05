using System.Data.Common;
using System.Threading.Tasks;

namespace Weasel.Core
{
    public static class ConnectionExtensions
    {
#if NETSTANDARD2_0
        /// <summary>
        /// Polyfill for Netstandard2 targets
        /// </summary>
        /// <param name="conn"></param>
        /// <returns></returns>
        public static Task CloseAsync(this DbConnection conn)
        {
            conn.Close();
            return Task.CompletedTask;
        }
#endif


    }
}
