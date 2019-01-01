#region

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using HC.Core.Events;
using HC.Core.Helpers;
using HC.Core.Logging;
using HC.Core.Text;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public class SqlDataLoader
    {
        #region Members

        private static readonly ILoggerService m_lc = Logger.GetLogger();

        #endregion

        public static DataWrapper LoadSqlTokenDataArray(
            string strDbName,
            string strTableName,
            SqlUniversalDbService dbService,
            List<string> fieldList,
            List<string> whereList,
            List<object> whereValuesList)
        {
            var fieldCount = fieldList.Count;
            if (fieldCount == 0)
            {
                return null;
            }
            try
            {
                SendMessageEvent.OnSendMessage("Counting rows. Please wait...", 0);
                var tableList = new List<string>();
                tableList.Add(strTableName);

                var recordCount =
                    dbService.GetRowCount(
                        strDbName,
                        tableList,
                        whereList,
                        whereValuesList);

                // create the output array
                var outputArray =
                    new DataWrapper(
                        new RowWrapper[recordCount]);
                var orderByList = new List<string>();
                orderByList.Add(fieldList[0]);
                using (var reader = dbService.GetDataReader(
                    strDbName,
                    tableList,
                    fieldList,
                    whereList,
                    whereValuesList,
                    null,
                    orderByList))
                {
                    LoadTokens(
                        fieldCount,
                        outputArray,
                        reader);
                }
                return outputArray;
            }
            catch (Exception e2)
            {
                m_lc.Write(e2);
                PrintToScreen.WriteLine(e2.Message);
                return null;
            }
        }

        private static void LoadTokens(
            int fieldCount,
            DataWrapper outputArray,
            DbDataReaderWrapper reader)
        {
            var rowIndex = 0;
            while (reader.Read())
            {
                outputArray.Data[rowIndex] = new RowWrapper();
                outputArray.Data[rowIndex].Columns = new TokenWrapper[fieldCount][];
                for (var fieldIndex = 0; fieldIndex < fieldCount; fieldIndex++)
                {
                    outputArray.Data[rowIndex].Columns[fieldIndex] =
                        Tokeniser.TokeniseAndWrap(reader[fieldIndex].ToString());
                }
                rowIndex++;
            }
        }

        public static DataWrapper LoadSqlTokenDataArray(
            string strDbName,
            string strTableName,
            SqlConnectionStringBuilder connectionString,
            List<string> fieldList)
        {
            var fieldCount = fieldList.Count;
            if (fieldCount == 0)
            {
                return null;
            }
            try
            {
                SendMessageEvent.OnSendMessage("Counting rows. Please wait...", 0);
                var recordCount =
                    SqlCommandHelper.GetRowCount(
                        strTableName,
                        connectionString);

                // create the output array
                var outputArray =
                    new DataWrapper(
                        new RowWrapper[recordCount]);
                using (var dbService = new SqlUniversalDbService(
                    connectionString.DataSource,
                    connectionString.InitialCatalog,
                    connectionString.ConnectionString))
                {
                    var orderByList = new List<string>();
                    orderByList.Add(fieldList[0]);
                    using (var reader = dbService.GetDataReader(
                        strDbName,
                        new List<string>(new[] {strTableName}),
                        fieldList,
                        null,
                        null,
                        null,
                        orderByList))
                    {
                        LoadTokens(
                            fieldCount,
                            outputArray,
                            reader);
                    }
                }
                return outputArray;
            }
            catch (Exception e2)
            {
                m_lc.Write(e2);
                PrintToScreen.WriteLine(e2.Message);
                return null;
            }
        }

        /// <summary>
        ///   Read SQL data table;
        ///   Load data into a referenced array.
        /// </summary>
        /// <param name = "strDbTableArray"></param>
        /// <param name = "intRowIndex"></param>
        /// <param name = "strDbName"></param>
        /// <param name = "strTableName"></param>
        /// <param name = "fieldList"></param>
        /// <param name = "connectionString"></param>
        /// <param name = "intColumnCount"></param>
        public static void LoadDataIntoArray(
            ref string[][][] strDbTableArray,
            int intRowIndex,
            string strDbName,
            string strTableName,
            List<string> fieldList,
            SqlConnectionStringBuilder connectionString,
            int intColumnCount)
        {
            var columnString = fieldList[0];
            for (var i = 1; i < fieldList.Count; i++)
            {
                var currentString = fieldList[i];
                columnString = columnString + ", " + currentString;
            }

            try
            {
                var strSqlStatement1 = "USE " + strDbName + " SELECT " +
                                       columnString + " FROM " + strTableName;

                var reader = SqlCommandHelper.GetReader(
                    strSqlStatement1,
                    connectionString);

                while (reader.Read())
                {
                    strDbTableArray[intRowIndex] = new string[intColumnCount][];
                    for (var columnIndex = 0; columnIndex < intColumnCount; columnIndex++)
                    {
                        strDbTableArray[intRowIndex][columnIndex] =
                            Tokeniser.Tokenise(reader[columnIndex].ToString(), false);
                    }
                    intRowIndex++;
                }
                reader.Close();
            }
            catch (Exception e)
            {
                m_lc.Write(e);
                PrintToScreen.WriteLine(e.Message);
            }
        }

        public static string[][][] LoadSqlDataArray(
            string strDbName1,
            string strDbName2,
            string strTable1,
            string strTable2,
            SqlConnectionStringBuilder connectionString,
            List<string> fieldList)
        {
            var columnCount = fieldList.Count;
            if (columnCount == 0)
            {
                return null;
            }
            try
            {
                // count records in the two tables
                var recordCount1 =
                    SqlCommandHelper.GetRowCount(
                        strTable1,
                        connectionString);
                var recordCount2 =
                    SqlCommandHelper.GetRowCount(
                        strTable2,
                        connectionString);

                // create the output array
                var outputArray = new string[recordCount1 + recordCount2][][];

                // add records from database 1
                LoadDataIntoArray(
                    ref outputArray,
                    0,
                    strDbName1,
                    strTable1,
                    fieldList,
                    connectionString,
                    columnCount);

                // add records from database 2
                LoadDataIntoArray(
                    ref outputArray,
                    recordCount1,
                    strDbName2,
                    strTable2,
                    fieldList,
                    connectionString,
                    columnCount);

                return outputArray;
            }
            catch (Exception e2)
            {
                m_lc.Write(e2);
                PrintToScreen.WriteLine(e2.Message);
                return null;
            }
        }
    }
}
