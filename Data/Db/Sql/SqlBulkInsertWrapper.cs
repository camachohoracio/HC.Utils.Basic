#region

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using HC.Core.Events;
using HC.Core.Exceptions;
using HC.Core.Helpers;
using HC.Core.Io;
using HC.Core.Io.DataTables;
using HC.Core.Logging;
using HC.Core.Pooling;
using HC.Core.Reflection;
using HC.Core.Resources;
using HC.Utils.Basic.Data.Db.DbConnections;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    /// <summary>
    ///   Efficiently insert rows into SQL server
    /// </summary>
    public class SqlBulkInsertWrapper
    {
        #region Delegates

        public delegate void UpdateProgressEventHandler(
            string strMessage,
            int intPercentage);

        #endregion

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

        public SqlBulkInsertWrapper()
        {
            // set the defaults number of files as one
            m_intFileCount = 1;
            m_intFilesCompleted = 0;
        }

        #endregion

        #region Public

        public void BulkInsertList<T>(
            List<T> list,
            string strTableName,
            SqlConnectionStringBuilder connectionStringBuilder)
        {
            SqlCommandHelper.ValidateTable<T>(
                strTableName,
                connectionStringBuilder);

            DataTable dt = GetDataTable(list);
            BulkInsertMemoryDataTable(
                strTableName,
                dt,
                connectionStringBuilder);
        }

        private static DataTable GetDataTable<T>(
            List<T> list)
        {
            var binder = ReflectorCache.GetReflector(typeof(T));
            List<string> propertyNames = binder.GetPropertyNames();
            var dt = DataTableHelper.CreateDataTable(
                propertyNames.ToArray(),
                binder.GetPropertyTypes().ToArray());

            foreach (T tObj in list)
            {
                var dataRow = dt.NewRow();
                foreach (string strPropertyName in propertyNames)
                {
                    dataRow[strPropertyName] =
                        binder.GetPropertyValue(tObj, strPropertyName);
                }
                dt.Rows.Add(dataRow);
            }
            return dt;
        }

        public void BulkInsertMemoryDataTable(
            string strTableName,
            DataTable dataTable,
            SqlConnectionStringBuilder connectionString)
        {
            ResourcePool resoucePool = null;
            DbConnectionWrapper connection = null;

            try
            {
                resoucePool = ResourcePool.GetInstance(typeof(DbConnectionFactory));

                var sqlConnectionStringBuilder =
                    new SqlConnectionStringBuilder(connectionString.ConnectionString);

                var dbDataRequest =
                    new DbDataRequest(
                        sqlConnectionStringBuilder.DataSource,
                        sqlConnectionStringBuilder.InitialCatalog);

                connection = resoucePool.Reserve(
                    this,
                    dbDataRequest) as DbConnectionWrapper;

                if (connection != null)
                {
                    connection.ChangeDatabase(connectionString.InitialCatalog);

                    BulkInsertMemoryDataTable(
                        strTableName,
                        dataTable,
                        (connection.DbConnection as SqlConnection));
                }
            }
            catch (Exception ex)
            {
                m_lc.Write(ex);
                m_blnCancelImport = true;
                PrintToScreen.WriteLine("Error executing bulk copy. " + ex.Message);
                throw new HCException(
                    "Error executing bulk copy. Table: " +
                    strTableName + ". Connection: " +
                    connectionString.ConnectionString + ". " +
                    ex.Message);
            }
            finally
            {
                if (connection != null)
                {
                    resoucePool.Release(connection);
                }
            }
        }


        public void BulkInsertMemoryDataTable(
            string strTableName,
            DataTable dataTable,
            SqlConnection conn)
        {
            try
            {
                m_progress = -1;
                m_longRowCount = dataTable.Rows.Count;
                var bcp = new SqlBulkCopy(conn) { DestinationTableName = strTableName, BulkCopyTimeout = int.MaxValue };
                bcp.SqlRowsCopied += BcpSqlRowsCopied;
                bcp.NotifyAfter = 500;
                bcp.BatchSize = 1000;
                bcp.WriteToServer(dataTable);
                bcp.Close();
            }
            catch (Exception ex)
            {
                m_lc.Write(ex);
                m_blnCancelImport = true;
                PrintToScreen.WriteLine("Error executing bulk copy. " + ex.Message);
                throw new HCException(
                    "Error executing bulk copy. " + ex.Message);
            }
        }

        public void BulkInsertMemoryDataTable(
            string strTableName,
            DataTable dataTable,
            string strConnectionString)
        {
            BulkInsertMemoryDataTable(
                strTableName,
                dataTable,
                new SqlConnectionStringBuilder(strConnectionString));
        }

        public void BulkCopySqlDataTable(
            string strTableName,
            string strSqlStatement,
            SqlConnectionStringBuilder connectionString1,
            SqlConnectionStringBuilder connectionString2)
        {
            try
            {
                m_progress = -1;
                DbDataReader reader =
                    SqlCommandHelper.GetReader(
                        strSqlStatement,
                        connectionString1);
                InvokeUpdateProgress("Preparing data transfer. Please wait...", -1);
                m_longRowCount = SqlCommandHelper.GetRowCountFromSqlStatement(
                    strSqlStatement,
                    connectionString1);
                InvokeUpdateProgress("Importing data. Please wait...", -1);
                var bcp = new SqlBulkCopy(connectionString2.ConnectionString)
                              {
                                  DestinationTableName = strTableName,
                                  BulkCopyTimeout = int.MaxValue
                              };
                bcp.SqlRowsCopied += BcpSqlRowsCopied;
                bcp.NotifyAfter = 500;
                bcp.BatchSize = 1000;
                bcp.WriteToServer(reader);
                bcp.Close();
                reader.Close();
            }
            catch (Exception ex)
            {
                m_blnCancelImport = true;
                PrintToScreen.WriteLine("Error executing bulk copy. " + ex.Message);
                m_lc.Write(ex);
                throw new HCException(
                    "Error executing bulk copy. " + ex.Message);
            }
        }

        public void BulkCopyCsv(
            string strTableName,
            string strCsvFileName,
            SqlConnection connection,
            bool blnHasHeaders)
        {
            BulkCopyCsv2(
                strTableName,
                strCsvFileName,
                connection,
                blnHasHeaders);
        }

        public void BulkCopyCsv(
            string strTableName,
            string strCsvFileName,
            string connectionString,
            bool blnHasHeaders)
        {
            BulkCopyCsv2(
                strTableName,
                strCsvFileName,
                connectionString,
                blnHasHeaders);
        }

        public void BulkCopyCsv(
            string strTableName,
            List<string> csvFileList,
            string connectionString,
            bool blnHasHeaders,
            long longRowCount)
        {
            m_longRowCount = longRowCount;
            m_intFilesCompleted = 0;
            m_intFileCount = csvFileList.Count;
            foreach (string strFileName in csvFileList)
            {
                BulkCopyCsv2(strTableName,
                             strFileName,
                             connectionString,
                             blnHasHeaders);
                m_intFilesCompleted++;
            }
        }

        public void BulkCopyCsv(
            string strTableName,
            string strCsvFileName,
            string connectionString,
            bool blnHasHeaders,
            long longRowCount)
        {
            m_longRowCount = longRowCount;
            BulkCopyCsv2(strTableName,
                         strCsvFileName,
                         connectionString,
                         blnHasHeaders);
        }

        private void BulkCopyCsv2(
            string strTableName,
            string strCsvFileName,
            SqlConnection connection,
            bool blnHasHeaders)
        {
            BulkCopyTextFile(
                strTableName,
                strCsvFileName,
                connection,
                blnHasHeaders,
                ',');
        }

        private void BulkCopyCsv2(
            string strTableName,
            string strCsvFileName,
            string connectionString,
            bool blnHasHeaders)
        {
            BulkCopyTextFile(
                strTableName,
                strCsvFileName,
                connectionString,
                blnHasHeaders,
                ',');
        }

        public void BulkCopyTextFile(
            string strTableName,
            string strStrFileName,
            bool blnHasHeaders,
            char chrDelimiter,
            List<Type> colTypeList,
            string connectionString,
            long lngRowCount)
        {
            CreateTable(
                strTableName,
                strStrFileName,
                blnHasHeaders,
                chrDelimiter,
                colTypeList,
                connectionString);

            //
            // bulk copy data file
            //
            BulkCopyTextFile(
                strTableName,
                strStrFileName,
                connectionString,
                blnHasHeaders,
                chrDelimiter,
                lngRowCount);
        }

        public void BulkCopyTextFile(
            string strTableName,
            string strStrFileName,
            bool blnHasHeaders,
            string strHeaders,
            char chrDelimiter,
            List<Type> colTypeList,
            string connectionString,
            long lngRowCount)
        {
            CreateTable(
                strTableName,
                strHeaders,
                chrDelimiter,
                colTypeList,
                connectionString);

            //
            // bulk copy data file
            //
            BulkCopyTextFile(
                strTableName,
                strStrFileName,
                connectionString,
                blnHasHeaders,
                chrDelimiter,
                lngRowCount);
        }


        public void BulkCopyTextFile(
            string strTableName,
            string strStrFileName,
            bool blnHasHeaders,
            char chrDelimiter,
            List<Type> colTypeList,
            string connectionString)
        {
            if (!SqlCommandHelper.CheckTableExists(
                strTableName,
                new SqlConnectionStringBuilder(connectionString)))
            {
                CreateTable(
                    strTableName,
                    strStrFileName,
                    blnHasHeaders,
                    chrDelimiter,
                    colTypeList,
                    connectionString);
            }
            //
            // bulk copy data file
            //
            BulkCopyTextFile(
                strTableName,
                strStrFileName,
                connectionString,
                blnHasHeaders,
                chrDelimiter);
        }

        private static void CreateTable(
            string strTableName,
            string strHeaders,
            char chrDelimiter,
            List<Type> colTypeList,
            string connectionString)
        {
            var strTitles = strHeaders.Split(chrDelimiter);
            var columnTitleList = new List<string>(strTitles);
            //
            // create temp table
            //
            SqlCommandHelper.CreateTable(
                strTableName,
                colTypeList,
                columnTitleList,
                connectionString);
        }

        private static void CreateTable(
            string strTableName,
            string strStrFileName,
            bool blnHasHeaders,
            char chrDelimiter,
            List<Type> colTypeList,
            string connectionString)
        {
            List<string> columnTitleList = null;
            if (blnHasHeaders)
            {
                using (var sr = new StreamReader(strStrFileName))
                {
                    var readLine = sr.ReadLine();
                    if (readLine != null)
                    {
                        var strTitles = readLine.Split(chrDelimiter);
                        columnTitleList = new List<string>(strTitles);
                    }
                }
            }
            else
            {
                columnTitleList = new List<string>();
                for (var i = 0; i < colTypeList.Count; i++)
                {
                    columnTitleList.Add("col_" + (i + 1));
                }
            }

            //
            // create temp table
            //
            SqlCommandHelper.CreateTable(
                strTableName,
                colTypeList,
                columnTitleList,
                connectionString);
        }


        private void BulkCopyTextFile(
            string strTableName,
            string strStrFileName,
            SqlConnection connection,
            bool blnHasHeaders,
            char chrDelimiter)
        {
            try
            {
                const string strMessage = "Counting rows. Please wait...";
                PrintToScreen.WriteLine(strMessage);
                SendMessageEvent.OnSendMessage(this, strMessage);

                m_longRowCount = FileHelper.CountNumberOfRows(strStrFileName);
                m_progress = -1;
                var reader = new CsvReader(
                    new StreamReader(strStrFileName), blnHasHeaders, chrDelimiter);
                var bcp = new SqlBulkCopy(connection)
                              {
                                  DestinationTableName = strTableName,
                                  BulkCopyTimeout = int.MaxValue
                              };
                bcp.SqlRowsCopied += BcpSqlRowsCopied;
                bcp.NotifyAfter = 500;
                bcp.BatchSize = 1000;
                bcp.WriteToServer(reader);
                bcp.Close();
                reader.Dispose();
            }
            catch (Exception ex)
            {
                m_blnCancelImport = true;
                PrintToScreen.WriteLine("Error executing bulk copy. " + ex.Message);
                m_lc.Write(ex);
                throw new HCException(
                    "Error executing bulk copy. " + ex.Message);
            }
        }

        private void BulkCopyTextFile(
            string strTableName,
            string strStrFileName,
            string connectionString,
            bool blnHasHeaders,
            char chrDelimiter,
            long lngRowCount)
        {
            m_longRowCount = lngRowCount;

            DoBulkCopy(
                strTableName,
                strStrFileName,
                connectionString,
                blnHasHeaders,
                chrDelimiter);
        }

        private void BulkCopyTextFile(
            string strTableName,
            string strStrFileName,
            string connectionString,
            bool blnHasHeaders,
            char chrDelimiter)
        {
            const string strMessage = "Counting rows. Please wait...";
            PrintToScreen.WriteLine(strMessage);
            SendMessageEvent.OnSendMessage(this, strMessage);
            m_longRowCount = FileHelper.CountNumberOfRows(strStrFileName);

            DoBulkCopy(
                strTableName,
                strStrFileName,
                connectionString,
                blnHasHeaders,
                chrDelimiter);
        }

        private void DoBulkCopy(
            string strTableName,
            string strStrFileName,
            string connectionString,
            bool blnHasHeaders,
            char chrDelimiter)
        {
            try
            {
                m_progress = -1;
                var reader = new CsvReader(
                    new StreamReader(strStrFileName), blnHasHeaders, chrDelimiter);
                var bcp = new SqlBulkCopy(connectionString)
                              {
                                  DestinationTableName = strTableName,
                                  BulkCopyTimeout = int.MaxValue
                              };
                bcp.SqlRowsCopied += BcpSqlRowsCopied;
                bcp.NotifyAfter = 500;
                bcp.BatchSize = 1000;
                bcp.WriteToServer(reader);
                bcp.Close();
                reader.Dispose();
            }
            catch (Exception ex)
            {
                m_blnCancelImport = true;
                PrintToScreen.WriteLine("Error executing bulk copy. " + ex.Message);
                m_lc.Write(ex);
                throw new HCException(
                    "Error executing bulk copy. " + ex.Message);
            }
        }

        public void BulkCopyCsv(
            string strTableName,
            string strCsvFileName,
            SqlConnection connection)
        {
            BulkCopyCsv(
                strTableName,
                strCsvFileName,
                connection,
                false);
        }

        public void BulkCopyCsv(
            string strTableName,
            string strCsvFileName,
            string connectionString)
        {
            BulkCopyCsv(
                strTableName,
                strCsvFileName,
                connectionString,
                false);
        }

        /// <summary>
        ///   Cancel import process
        /// </summary>
        public void CancelImport()
        {
            m_blnCancelImport = true;
        }

        #endregion

        #region Private

        /// <summary>
        ///   Call this method each time a bulk 
        ///   insert batch is completed
        /// </summary>
        /// <param name = "sender">
        ///   Sender
        /// </param>
        /// <param name = "e">
        ///   Event arguments
        /// </param>
        private void BcpSqlRowsCopied(object sender, SqlRowsCopiedEventArgs e)
        {
            if (m_blnCancelImport)
            {
                e.Abort = true;
            }
            var rowsCopied = e.RowsCopied;
            var currentProgress =
                ((100 * m_intFilesCompleted) / m_intFileCount) +
                (int)((rowsCopied * 100) / m_longRowCount);
            var strMessage = "Copied so far..." + rowsCopied +
                             " rows. Percentage completed: " +
                             currentProgress;
            if (m_progress != currentProgress)
            {
                m_progress = currentProgress;
                PrintToScreen.WriteLine(strMessage);
                PrintToScreen.WriteLine(strMessage);
                SendMessageEvent.OnSendMessage(this, strMessage);
                InvokeUpdateProgress(strMessage, currentProgress);
            }
        }

        private void InvokeUpdateProgress(string strMessage, int intPercentage)
        {
            if (UpdateProgress != null)
            {
                if (UpdateProgress.GetInvocationList().Length > 0)
                {
                    UpdateProgress(strMessage, intPercentage);
                }
            }
        }

        #endregion

        public event UpdateProgressEventHandler UpdateProgress;
    }
}
