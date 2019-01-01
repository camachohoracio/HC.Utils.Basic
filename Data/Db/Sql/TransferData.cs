#region

using System;
using System.Data.SqlClient;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public class TransferData : IDisposable
    {
        #region events

        #region Delegates

        public delegate void FinishProcessEventHandler();

        public delegate void SendMessageEventHandler(
            string strMessage,
            int intProgress);

        #endregion

        public event SendMessageEventHandler SendMessage;

        public event FinishProcessEventHandler FinishProcess;

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            HC.Core.EventHandlerHelper.RemoveAllEventHandlers(this);
        }

        #endregion

        ~TransferData()
        {
            Dispose();
        }

        public void TransferAllTables(
            SqlConnectionStringBuilder connectionStringSoruce,
            SqlConnectionStringBuilder connectionStringDestination,
            bool blnDeleteTable,
            int intRowCount)
        {
            var tableList =
                SqlCommandHelper.GetTableList(connectionStringSoruce);
            foreach (string strTableName in tableList)
            {
                var strSqlStatement = "";
                if (intRowCount > 0)
                {
                    strSqlStatement =
                        SqlStatementHelper.GetSelectTopFromTableStatement(
                            strTableName,
                            intRowCount);
                }
                else
                {
                    strSqlStatement =
                        SqlStatementHelper.GetSelectAllFromTableStatement(
                            strTableName);
                }
                TransferDataTable(
                    blnDeleteTable,
                    connectionStringSoruce,
                    connectionStringDestination,
                    strTableName,
                    strSqlStatement);
            }
            InvokeFinishProcess();
        }

        public void TransferDataTable(
            string strTableName,
            string strSqlStatement,
            SqlConnectionStringBuilder connectionStringSoruce,
            SqlConnectionStringBuilder connectionStringDestination)
        {
            var bulkInsert = new SqlBulkInsertWrapper();
            bulkInsert.UpdateProgress +=
                InvokeSendMessage;
            bulkInsert.BulkCopySqlDataTable(
                strTableName,
                strSqlStatement,
                connectionStringSoruce,
                connectionStringDestination);
            InvokeFinishProcess();
            Dispose();
        }

        public void TransferDataTable(
            bool blnDeleteTable,
            SqlConnectionStringBuilder sourceConnectionString,
            SqlConnectionStringBuilder destinationConnectionString,
            string strTableName,
            string strSqlStatement)
        {
            if (blnDeleteTable)
            {
                InvokeSendMessage(
                    "Deleting content from table: " +
                    strTableName + ". Please wait...",
                    -1);
                SqlCommandHelper.TruncateTable(
                    strTableName,
                    destinationConnectionString);
            }
            TransferDataTable(
                strTableName,
                strSqlStatement,
                sourceConnectionString,
                destinationConnectionString);
            InvokeFinishProcess();
        }

        #region InvokeMethods

        private void InvokeSendMessage(
            string strMessage,
            int intProgress)
        {
            if (SendMessage != null)
            {
                if (SendMessage.GetInvocationList().Length > 0)
                {
                    SendMessage.Invoke(
                        strMessage,
                        intProgress);
                }
            }
        }

        private void InvokeFinishProcess()
        {
            if (FinishProcess != null)
            {
                if (FinishProcess.GetInvocationList().Length > 0)
                {
                    FinishProcess.Invoke();
                }
            }
        }

        #endregion
    }
}
