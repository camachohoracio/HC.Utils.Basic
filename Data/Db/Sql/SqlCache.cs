#region Members

using System.Collections.Concurrent;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public static class SqlCache
    {
        #region Members

        private static readonly ConcurrentDictionary<string, object> m_existingTables;

        #endregion

        #region Constructor

        static SqlCache()
        {
            m_existingTables = new ConcurrentDictionary<string, object>();
        }

        #endregion

        #region Public

        public static bool ContainsExistingTable(string strTable)
        {
            object obj;
            return m_existingTables.TryGetValue(strTable, out obj);
        }

        public static void AddToExistingTables(string strTableName)
        {
            m_existingTables[strTableName] = null;
        }

        #endregion
    }
}

