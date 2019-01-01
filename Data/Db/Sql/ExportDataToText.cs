#region

using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using HC.Core.Io;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public class ExportDataToText
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

        public void ExportDataToFile(
            string strSqlStatement,
            string strConnectionString,
            string strFileName,
            char delimiter)
        {
            var strDirName = new FileInfo(strFileName).DirectoryName;
            if (!DirectoryHelper.Exists(strDirName))
            {
                DirectoryHelper.CreateDirectory(strDirName);
            }
            var connectionString =
                new SqlConnectionStringBuilder(strConnectionString);
            long longRowCount =
                SqlCommandHelper.GetRowCountFromSqlStatement(
                    strSqlStatement,
                    connectionString);
            var sw = new StreamWriter(strFileName);

            DbDataReader reader = SqlCommandHelper.GetReader(
                strSqlStatement,
                connectionString);
            long longLineCounter = 0;
            var intPreviousPercentage = -1;
            while (reader.Read())
            {
                var strRow = Convert.ToString(reader[0]);

                // calculate percentage progress
                longLineCounter++;
                var intPercentage = (int) ((100*longLineCounter)/longRowCount);
                if (intPreviousPercentage != intPercentage)
                {
                    InvokeSendMessage(
                        "Exporting table to file: " + strFileName,
                        intPercentage);
                }
                intPreviousPercentage = intPercentage;

                // read each field
                for (var i = 1; i < reader.FieldCount; i++)
                {
                    //
                    // replace delimit by a space
                    //
                    var strDescr = Convert.ToString(
                        reader[i]).Replace(delimiter, ' ');
                    strDescr = strDescr.Replace("\t", " ")
                        .Replace("\r", " ")
                        .Replace("\n", " ")
                        .Replace(Environment.NewLine, " ");
                    strRow += delimiter + strDescr;
                }
                sw.WriteLine(strRow);
            }

            // tidy up
            reader.Close();
            sw.Close();
            InvokeFinishProcess();
        }

        public void ExportDataToCsv(
            string strSqlStatement,
            string strConnectionString,
            string strFileName)
        {
            ExportDataToFile(strSqlStatement, strConnectionString, strFileName, ',');
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
