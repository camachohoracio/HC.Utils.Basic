#region

using HC.Core.Logging;

#endregion

namespace HC.Utils.Basic.Data.Db.Oracle
{
    public class OracleBulkInsert
    {
        #region Members

        private static readonly ILoggerService m_lc = Logger.GetLogger();

        /// <summary>
        ///   Cancel the import process
        /// </summary>
        private bool m_blnCancelImport;

        private int m_intFileCount;
        private int m_intFilesCompleted;
        private long m_longRowCount;
        private int m_progress;

        #endregion

        #region Constructor

        public OracleBulkInsert()
        {
            // set the defaults number of files as one
            m_intFileCount = 1;
            m_intFilesCompleted = 0;
        }

        #endregion
    }
}
