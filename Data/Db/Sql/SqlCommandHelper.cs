#region

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using HC.Core.Events;
using HC.Core.Io;
using HC.Core.Logging;
using HC.Core.Reflection;
using HC.Utils.Basic.Data.Db.DbConnections;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public static class SqlCommandHelper
    {
        #region Members

        private static readonly object m_tableCheckValidator = new object();

        #endregion

        public static Dictionary<string, object> GetReaderFields(DbDataReaderWrapper dbDataReaderWrapper)
        {
            var fieldMap =
                new Dictionary<string, object>();
            for (var i = 0; i < dbDataReaderWrapper.FieldCount; i++)
            {
                fieldMap.Add(
                    dbDataReaderWrapper.GetName(i),
                    dbDataReaderWrapper[i]);
            }
            return fieldMap;
        }

        public static DbDataReaderWrapper GetReader(
            string strQuery,
            SqlConnectionStringBuilder sqlConnectionStringBuilder)
        {
            //
            // register server to resource pool
            //
            DbConnectionService.AddServerToResourcePool(
                sqlConnectionStringBuilder.DataSource,
                sqlConnectionStringBuilder.InitialCatalog,
                sqlConnectionStringBuilder.ConnectionString,
                DbProviderType.SQL);

            var conn =
                DbConnectionService.Reserve(
                    sqlConnectionStringBuilder.DataSource,
                    sqlConnectionStringBuilder.InitialCatalog,
                    sqlConnectionStringBuilder.ConnectionString,
                    DbProviderType.SQL);

            return conn.ExecuteReader(strQuery);
        }

        public static List<string> GetColumnList(
            string strTableName,
            SqlConnectionStringBuilder connectionString)
        {
            var strCommand = SqlStatementHelper.GetColumnsStatement(
                connectionString.InitialCatalog,
                strTableName);
            return LoadListFromQuery(
                connectionString,
                strCommand,
                3);
        }

        public static List<string> GetDbList(
            SqlConnectionStringBuilder connectionString)
        {
            var strCommand = SqlStatementHelper.GetDbList();
            return LoadListFromQuery(
                connectionString,
                strCommand,
                0);
        }

        public static List<string> GetTempTableList(
            SqlConnectionStringBuilder connectionString)
        {
            return GetTempSysOBjectList(connectionString, "U");
        }

        public static List<string> GetTableList(
            SqlConnectionStringBuilder connectionString)
        {
            return GetSysOBjectList(
                connectionString,
                "U");
        }

        public static List<string> GetViewList(
            SqlConnectionStringBuilder connectionString)
        {
            return GetSysOBjectList(connectionString, "V");
        }

        private static List<string> GetSysOBjectList(
            SqlConnectionStringBuilder connectionString,
            string strObjectType)
        {
            var queryBuilder = new SqlQueryBuilder();
            queryBuilder.AddField("[name]");
            queryBuilder.AddTable("sysobjects");
            var strSqlStatement = queryBuilder.SelectQuery +
                                  "WHERE type = " +
                                  "'" + strObjectType + "'";

            return LoadListFromQuery(
                connectionString,
                strSqlStatement,
                0);
        }

        private static List<string> LoadListFromQuery(
            SqlConnectionStringBuilder connectionString,
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

        private static List<string> GetTempSysOBjectList(
            SqlConnectionStringBuilder connectionString,
            string strObjectType)
        {
            var queryBuilder = new SqlQueryBuilder();
            queryBuilder.AddField("[name]");
            queryBuilder.AddTable("tempdb..sysobjects");
            var strSqlStatement = queryBuilder.SelectQuery +
                                  "WHERE type = " +
                                  "'" + strObjectType + "'";

            return LoadListFromQuery(
                connectionString,
                strSqlStatement,
                0);
        }

        public static void ExecuteSqlFile(
            string strFileName,
            SqlConnectionStringBuilder connectionString)
        {
            var strDosCommand = "sqlcmd -S " +
                                connectionString.DataSource + " -E -i " +
                                '"' + strFileName + '"';
            FileHelper.ExecuteDosCommand(strDosCommand);
        }

        public static int GetRowCount(
            string strTableName,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetRowCountStatement(strTableName);
            return ExecuteValue<int>(strQuery, connectionString);
        }

        public static int GetRowCountFromSqlStatement(
            string strSqlStatement,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetSelectCountFromSqlStatement(
                    strSqlStatement);
            return ExecuteValue<int>(strQuery, connectionString);
        }

        public static bool CheckTempTableExists(
            string dbTableName,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery = "IF OBJECT_ID('tempdb.." + dbTableName + "','u') IS NOT NULL " +
                           "SELECT 1 " +
                           "ELSE " +
                           "SELECT 0";
            return ExecuteValue<int>(strQuery, connectionString) == 1;
        }

        public static void ValidateTable<T>(
            string strTableName,
            SqlConnectionStringBuilder connectionString)
        {
            lock (m_tableCheckValidator)
            {
                if (!SqlCache.ContainsExistingTable(strTableName) &&
                    !CheckTableExists(strTableName, connectionString))
                {
                    var binder = ReflectorCache.GetReflector(typeof (T));
                    CreateTable(
                        strTableName,
                        binder.GetPropertyTypes(),
                        binder.GetPropertyNames(),
                        connectionString.ConnectionString);
                    SqlCache.AddToExistingTables(strTableName);
                }
            }
        }

        public static bool CheckTableExists(
            string dbTableName,
            SqlConnectionStringBuilder connectionString)
        {
            var tableList = GetTableList(connectionString);
            return tableList.Contains(dbTableName
                                          .Replace("[", string.Empty)
                                          .Replace("]", string.Empty)
                                          .ToLower()
                                          .Trim());
        }

        public static bool CheckViewExists(
            string dbViewName,
            SqlConnectionStringBuilder connectionString)
        {
            var viewList = GetViewList(connectionString);
            return viewList.Contains(dbViewName);
        }

        public static void TruncateTable(
            string strTableName,
            SqlConnectionStringBuilder connectionString)
        {
            var tableBuilder =
                new SqlTableBuilder(strTableName);
            var strQuery =
                tableBuilder.SqlTruncateTable;
            ExecuteNonQuery(strQuery, connectionString);
        }

        public static bool CheckDbExists(
            string strDbName,
            SqlConnectionStringBuilder connectionString)
        {
            try
            {
                var connStr = new SqlConnectionStringBuilder(
                    connectionString.ConnectionString);
                connStr.InitialCatalog = "master";
                // check if the database exists
                var sqlStr = SqlStatementHelper.GetExistDatabaseStatement(
                    strDbName);
                var result = ExecuteValue<int>(sqlStr, connStr);
                if (result == 1)
                {
                    return true;
                }
                else
                {
                    return false;
                }
            }
            catch
            {
                return false;
            }
        }


        public static void CreateView(
            string strSqlStatement,
            string strViewName,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetCreateViewStatement(
                    strSqlStatement,
                    strViewName);
            ExecuteNonQuery(strQuery,
                            connectionString);
        }

        public static bool AttachDB(
            string strDbName,
            string strMdfRelativeFileName,
            string strLdfRelativeFileName,
            string strMdfNetworkFileName,
            string strLdfNetworkFileName,
            SqlConnectionStringBuilder connectionString)
        {
            try
            {
                // create directory in case it doesn't exist
                var strMdfDirName = new FileInfo(strMdfNetworkFileName).DirectoryName;
                var strLdfDirName = new FileInfo(strLdfNetworkFileName).DirectoryName;
                if (!DirectoryHelper.Exists(strMdfDirName))
                {
                    DirectoryHelper.CreateDirectory(strMdfDirName);
                }
                if (!DirectoryHelper.Exists(strLdfDirName))
                {
                    DirectoryHelper.CreateDirectory(strLdfDirName);
                }


                var newSqlConnectionStringBuilder =
                    new SqlConnectionStringBuilder(
                        connectionString.ConnectionString);
                newSqlConnectionStringBuilder.InitialCatalog = "master";
                var strQuery =
                    SqlStatementHelper.GetAttachDbStatement(
                        strDbName,
                        strMdfNetworkFileName,
                        strLdfNetworkFileName,
                        strMdfRelativeFileName,
                        strLdfRelativeFileName);
                ExecuteNonQuery(strQuery,
                                newSqlConnectionStringBuilder);
            }
            catch (Exception e)
            {
                var strMessage = "Attach error. ";
                //m_lc.Write(strMessage);
                //lc.Write(e);
                Console.WriteLine(strMessage, e);
                return false;
            }
            return true;
        }


        public static bool AttachDB(
            string strDbName,
            string strPath,
            SqlConnectionStringBuilder connectionString)
        {
            return AttachDB(
                strDbName,
                strPath,
                strPath,
                connectionString);
        }

        public static bool AttachDB(
            string strDbName,
            string strRelativePath,
            string strNetworkPath,
            SqlConnectionStringBuilder connectionString)
        {
            // make up default mdf and log files
            var strMdfDefaultName =
                Helper.GetDefaultMdfFileName(
                    strDbName);
            var strLdfDefaultName =
                Helper.GetDefaultLdfFileName(
                    strDbName);

            var strMdfNetworkFileName =
                strNetworkPath + @"\" +
                strMdfDefaultName;

            var strLdfNetworkFileName =
                strNetworkPath + @"\" +
                strLdfDefaultName;

            var strMdfRelativeFileName =
                strRelativePath + @"\" +
                strMdfDefaultName;

            var strLdfRelativeFileName =
                strRelativePath + @"\" +
                strLdfDefaultName;

            return AttachDB(
                strDbName,
                strMdfRelativeFileName,
                strLdfRelativeFileName,
                strMdfNetworkFileName,
                strLdfNetworkFileName,
                connectionString);
        }


        public static void SwapConnectionToDatabase(
            string strDbName,
            DbConnectionWrapper connection)
        {
            var strQuery =
                SqlStatementHelper.GetUseDbStatement(strDbName);
            var conn = (IDbConnection) connection;
            var cmd =
                DbCommandFactory.BuildDbCommand(
                    DbProviderType.SQL,
                    strQuery,
                    conn as DbConnection);
            cmd.ExecuteNonQuery();
            cmd.Dispose();
        }

        public static SqlConnectionStringBuilder CreateConnectionStringBuilder(
            string strServerName)
        {
            var connectionString = new SqlConnectionStringBuilder();
            connectionString.InitialCatalog = "master";
            connectionString.DataSource = strServerName;
            connectionString.IntegratedSecurity = true;
            return connectionString;
        }

        public static bool CreateDatabase(
            string strDbName,
            string strPath,
            SqlConnectionStringBuilder connectionString)
        {
            var strMdfFileName =
                strPath + @"\" +
                Helper.GetDefaultMdfFileName(
                    strDbName);
            var strLdfFileName =
                strPath + @"\" +
                Helper.GetDefaultLdfFileName(
                    strDbName);
            return CreateDatabase(
                strDbName,
                strMdfFileName,
                strLdfFileName,
                connectionString);
        }

        public static bool CreateDatabase(
            string strDbName,
            string strMdfFileName,
            string strLdfFileName,
            SqlConnectionStringBuilder connectionString)
        {
            if (!DbCheckService.CheckNewDb(
                connectionString,
                strDbName,
                strMdfFileName,
                strLdfFileName))
            {
                return false;
            }

            var fi = new FileInfo(strMdfFileName);
            if (!DirectoryHelper.Exists(fi.DirectoryName))
            {
                DirectoryHelper.CreateDirectory(fi.DirectoryName);
            }

            fi = new FileInfo(strLdfFileName);
            if (!DirectoryHelper.Exists(fi.DirectoryName))
            {
                DirectoryHelper.CreateDirectory(fi.DirectoryName);
            }

            var strQuery =
                SqlStatementHelper.GetCreateDatabaseStatement(
                    strDbName,
                    strMdfFileName,
                    strLdfFileName);
            ExecuteNonQuery(strQuery,
                            connectionString.ConnectionString,
                            false);
            return true;
        }

        public static bool CreateDatabase(
            string strDbName,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetCreateDatabaseStatement(
                    strDbName);
            ExecuteNonQuery(strQuery,
                            connectionString.ConnectionString,
                            false);
            return true;
        }


        public static void RenameDbName(
            string strOldDbName,
            string strNewDbName,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetRenameDbNameStatement(
                    strOldDbName,
                    strNewDbName);

            ExecuteNonQuery(strQuery,
                            connectionString);
        }

        public static void RenameLogicalDbName(
            string strOldDbName,
            string strNewDbName,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetRenameLogicalDbNameStatement(
                    strOldDbName,
                    strNewDbName);

            ExecuteNonQuery(strQuery,
                            connectionString);
        }


        public static void DropAllTables(
            SqlConnectionStringBuilder connectionString)
        {
            var tableList = GetTableList(
                connectionString);

            foreach (string strTableName in tableList)
            {
                DropTable(
                    strTableName,
                    connectionString);
            }
        }

        public static void DropTable(
            string strTableName,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetDropTableStatement(strTableName);
            ExecuteNonQuery(strQuery,
                            connectionString);
        }

        public static void DropView(
            string strViewName,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetDropViewStatement(strViewName);
            ExecuteNonQuery(strQuery,
                            connectionString);
        }

        public static void RenameTable(
            string strOldTableName,
            string strNewTableName,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetRenameTableStatement(
                    strOldTableName,
                    strNewTableName);
            ExecuteNonQuery(strQuery,
                            connectionString);
        }

        public static void ShinkDatabase(
            string strDbName,
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetShrinkDbStatement(
                    strDbName);
            ExecuteNonQuery(strQuery,
                            connectionString);
        }

        public static string GetDefaultServerName()
        {
            var serverList = GetCandidateSeverList();

            foreach (string serverName in serverList)
            {
                var connStrBuilder =
                    SqlConnectionStringHelper.GetTrustedConnectionStringBuilder(
                        serverName);
                if (CheckServerExists(
                    connStrBuilder))
                {
                    return serverName;
                }
            }
            return "";
        }

        private static List<string> GetCandidateSeverList()
        {
            var strServerList = new List<string>();
            strServerList.Add("(local)");
            strServerList.Add(@"(local)\sql2005");
            strServerList.Add(@"(local)\sqlexpress");
            return strServerList;
        }

        public static bool DetatchDb(
            string strDbName,
            SqlConnectionStringBuilder connectionString)
        {
            try
            {
                connectionString =
                    new SqlConnectionStringBuilder(
                        connectionString.ConnectionString);
                connectionString.InitialCatalog = "master";
                var strQuery =
                    SqlStatementHelper.GetKillConnectionStatement(
                        strDbName);
                ExecuteNonQuery(strQuery,
                                connectionString);

                strQuery =
                    SqlStatementHelper.GetDetachDbStatement(
                        strDbName);
                ExecuteNonQuery(strQuery,
                                connectionString);
            }
            catch (Exception e2)
            {
                Logger.Log(e2);
                return false;
            }
            return true;
        }


        public static bool EnableClr(
            SqlConnectionStringBuilder connectionString)
        {
            try
            {
                var strQuery =
                    SqlStatementHelper.GetEnableClrStatement();
                ExecuteNonQuery(strQuery,
                                connectionString);
            }
            catch
            {
                return false;
            }
            return true;
        }

        public static bool CheckServerExists(
            SqlConnectionStringBuilder connStrBuilder)
        {
            try
            {
                SendMessageEvent.OnSendMessage(
                    null,
                    "Connecting to server: " + connStrBuilder.DataSource + "...");
                GetSysOBjectList(
                    connStrBuilder,
                    "V");
            }
            catch
            {
                return false;
            }
            return true;
        }

        public static double CheckRmsVer(
            SqlConnectionStringBuilder connectionString)
        {
            var strQuery =
                SqlStatementHelper.GetRmsVerStatement();
            return ExecuteValue<double>(strQuery,
                                        connectionString);
        }

        public static void ExecuteBatchQuery(
            string strQuery,
            string strConnectionString)
        {
            var sb = new StringBuilder();
            using (var sr = new StringReader(strQuery))
            {
                string strLine;
                while ((strLine = sr.ReadLine()) != null)
                {
                    //try
                    //{
                    if (strLine.ToUpper().Trim().Equals("GO"))
                    {
                        if (!sb.ToString().Equals(string.Empty))
                        {
                            ExecuteNonQuery(
                                sb.ToString(),
                                strConnectionString);
                            sb = new StringBuilder();
                        }
                    }
                        //else if (TextHelper.ContainsWord(strLine.ToLower(), "use"))
                        //{
                        //    if (!sb.ToString().Equals(string.Empty))
                        //    {
                        //        ExecuteNonQuery(
                        //            sb.ToString(),
                        //            connection);
                        //        sb = new StringBuilder();
                        //    }

                        //    // execute use statement
                        //    ExecuteNonQuery(
                        //        strLine,
                        //        connection);
                        //}
                    else
                    {
                        strLine =
                            SqlStatementHelper.ParseSqlStatement(
                                strLine);
                        sb.AppendLine(strLine);
                    }
                    //}
                    //catch (Exception e2)
                    //{
                    //    System.Diagnostics.//Debugger.Break();
                    //}
                }
            }
        }

        public static void AddColumn(
            string strTableName,
            Type columnType,
            string strColumnName,
            string strConnectionString)
        {
            var strQuery =
                SqlStatementHelper.GetAddColumnStatement(
                    strTableName,
                    columnType,
                    strColumnName);
            ExecuteNonQuery(strQuery, strConnectionString);
        }

        public static void CreateTable(
            string strTableName,
            List<Type> columnTypeList,
            List<string> columnNameList,
            string strConnectionString)
        {
            var strQuery =
                SqlStatementHelper.GetCreateTableStatement(
                    strTableName,
                    columnTypeList,
                    columnNameList);
            ExecuteNonQuery(strQuery, strConnectionString);
        }

        public static void DropGenericIndex(
            string strDbName,
            string strTableName,
            string strConnectionString)
        {
            var strQuery = SqlStatementHelper.DropGenericIndex(
                strDbName,
                strTableName);
            ExecuteNonQuery(strQuery, strConnectionString);
        }

        #region Private

        public static void ExecuteNonQuery(
            string strQuery,
            SqlConnectionStringBuilder connectionString)
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
                new SqlConnectionStringBuilder(strConnectionString);
            //
            // register server to resource pool
            //
            DbConnectionService.AddServerToResourcePool(
                connectionString.DataSource,
                connectionString.InitialCatalog,
                connectionString.ConnectionString,
                DbProviderType.SQL);

            //
            // get connection from pool
            //
            var conn =
                DbConnectionService.Reserve(
                    connectionString.DataSource,
                    connectionString.InitialCatalog,
                    connectionString.ConnectionString,
                    DbProviderType.SQL);

            conn.ExecuteNonQuery(
                strQuery,
                blnIsTransacion);
            //
            // release connection
            //
            DbConnectionService.Release(
                conn);
        }

        public static T ExecuteValue<T>(
            string strQuery,
            string strConnectionString)
        {
            var connectionString =
                new SqlConnectionStringBuilder(
                    strConnectionString);
            return ExecuteValue<T>(strQuery,
                                   connectionString);
        }

        public static T ExecuteValue<T>(
            string strQuery,
            SqlConnectionStringBuilder connectionString)
        {
            //
            // register server to resource pool
            //
            DbConnectionService.AddServerToResourcePool(
                connectionString.DataSource,
                connectionString.InitialCatalog,
                connectionString.ConnectionString,
                DbProviderType.SQL);
            //
            // get connection from poole
            //
            var conn =
                DbConnectionService.Reserve(
                    connectionString.DataSource,
                    connectionString.InitialCatalog,
                    connectionString.ConnectionString,
                    DbProviderType.SQL);

            var value = conn.SelectValue<T>(
                strQuery);

            DbConnectionService.Release(
                conn);

            return value;
        }

        #endregion
    }
}
