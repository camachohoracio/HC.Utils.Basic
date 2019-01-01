#region

using System;
using System.Collections.Generic;
using System.Data.OracleClient;
using System.IO;
using HC.Core.Helpers;
using HC.Utils.Basic.Data.Db.DbConnections;
using HC.Utils.Basic.Data.Db.Sql;

#endregion

namespace HC.Utils.Basic.Data.Db.Oracle
{
    public static class OracleCommandHelper
    {
        #region Public

        public static void TruncateTable(
            string strTableName,
            OracleConnectionStringBuilder connectionString)
        {
            var tableBuilder =
                new SqlTableBuilder(strTableName);
            var strQuery =
                tableBuilder.SqlTruncateTable;
            ExecuteNonQuery(strQuery, connectionString);
        }

        public static void CreateTable(
            string strTableName,
            string strFileName,
            string strConnectionString,
            char chrDelimiter,
            List<string> columnNameList)
        {
            var connectionString =
                new OracleConnectionStringBuilder(strConnectionString);

            if (!CheckTableExists(strTableName,
                                  connectionString))
            {
                var columnTypeList = new List<Type>();
                var blnAddTitles = false;
                if (columnNameList == null)
                {
                    columnNameList = new List<string>();
                    blnAddTitles = true;
                }

                var blnLoadFile = false;
                using (var reader = new StreamReader(strFileName))
                {
                    var strLine = reader.ReadLine();
                    if (strLine != null
                        && !strLine.Equals(string.Empty))
                    {
                        blnLoadFile = true;
                        var strTokenArr = strLine.Split(chrDelimiter);
                        var strFirstRowArr = reader.ReadLine().Split(chrDelimiter);
                        for (var i = 0; i < strFirstRowArr.Length; i++)
                        {
                            var strToken = strFirstRowArr[i];
                            var type = ParserHelper.GetType(strToken);
                            columnTypeList.Add(type);
                            if (blnAddTitles)
                            {
                                columnNameList.Add(
                                    strTokenArr[i].Replace(".", "_"));
                            }
                        }
                    }
                }
                if (blnLoadFile)
                {
                    CreateTable(
                        strTableName,
                        columnTypeList,
                        columnNameList,
                        connectionString);
                }
            }
        }

        public static void CreateTable(
            string strTableName,
            List<Type> columnTypeList,
            List<string> columnNameList,
            OracleConnectionStringBuilder connectionString)
        {
            var strQuery =
                OracleStatementHelper.GetCreateTableStatement(
                    strTableName,
                    columnTypeList,
                    columnNameList);
            ExecuteNonQuery(strQuery, connectionString);
        }

        public static bool CheckTableExists(
            string dbTableName,
            OracleConnectionStringBuilder connectionString)
        {
            var tableList = GetTableList(connectionString);
            return tableList.Contains(dbTableName.ToLower());
        }

        public static List<string> GetTableList(
            OracleConnectionStringBuilder connectionString)
        {
            var strQuery =
                OracleStatementHelper.SelectAllTableNames();
            return LoadListFromQuery(
                connectionString,
                strQuery,
                0);
        }


        public static DbDataReaderWrapper GetReader(
            string strQuery,
            OracleConnectionStringBuilder sqlConnectionStringBuilder)
        {
            //
            // register server to resource pool
            //
            DbConnectionService.AddServerToResourcePool(
                sqlConnectionStringBuilder.DataSource,
                "",
                sqlConnectionStringBuilder.ConnectionString,
                DbProviderType.ORACLE);

            var conn =
                DbConnectionService.Reserve(
                    sqlConnectionStringBuilder.DataSource,
                    "",
                    sqlConnectionStringBuilder.ConnectionString,
                    DbProviderType.ORACLE);

            return conn.ExecuteReader(strQuery);
        }

        #endregion

        #region Private

        private static List<string> LoadListFromQuery(
            OracleConnectionStringBuilder connectionString,
            string strSqlStatement,
            int intFieldIndex)
        {
            var resultList = new List<string>();
            using (var reader = GetReader(
                strSqlStatement,
                connectionString))
            {
                while (reader.Read())
                {
                    resultList.Add(Convert.ToString(reader[intFieldIndex]).ToLower());
                }
                reader.Close();
            }
            return resultList;
        }

        public static void ExecuteNonQuery(
            string strQuery,
            OracleConnectionStringBuilder connectionString)
        {
            ExecuteNonQuery(
                strQuery,
                connectionString.ConnectionString);
        }

        public static void ExecuteNonQuery(
            string strQuery,
            string strConnectionString)
        {
            ExecuteNonQuery(
                strQuery,
                strConnectionString,
                true);
        }

        public static void ExecuteNonQuery(
            string strQuery,
            string strConnectionString,
            bool blnIsTransacion)
        {
            var connectionString =
                new OracleConnectionStringBuilder(strConnectionString);
            //
            // register server to resource pool
            //
            DbConnectionService.AddServerToResourcePool(
                connectionString.DataSource,
                "",
                connectionString.ConnectionString,
                DbProviderType.ORACLE);

            //
            // get connection from pool
            //
            var conn =
                DbConnectionService.Reserve(
                    connectionString.DataSource,
                    "",
                    connectionString.ConnectionString,
                    DbProviderType.ORACLE);

            conn.ExecuteNonQuery(
                strQuery,
                blnIsTransacion);
            //
            // release connection
            //
            DbConnectionService.Release(
                conn);
        }

        private static T ExecuteValue<T>(
            string strQuery,
            string strConnectionString)
        {
            var connectionString =
                new OracleConnectionStringBuilder(
                    strConnectionString);
            return ExecuteValue<T>(strQuery,
                                   connectionString);
        }

        private static T ExecuteValue<T>(
            string strQuery,
            OracleConnectionStringBuilder connectionString)
        {
            //
            // register server to resource pool
            //
            DbConnectionService.AddServerToResourcePool(
                connectionString.DataSource,
                "",
                connectionString.ConnectionString,
                DbProviderType.ORACLE);
            //
            // get connection from poole
            //
            var conn =
                DbConnectionService.Reserve(
                    connectionString.DataSource,
                    "",
                    connectionString.ConnectionString,
                    DbProviderType.ORACLE);

            var value = conn.SelectValue<T>(
                strQuery);

            DbConnectionService.Release(
                conn);

            return value;
        }

        #endregion
    }
}
