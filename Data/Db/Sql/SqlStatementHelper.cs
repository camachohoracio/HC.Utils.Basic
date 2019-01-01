#region

using System;
using System.Collections.Generic;
using System.Text;
using HC.Analytics.Mathematics;
using HC.Core.Exceptions;
using HC.Core.Io;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public static class SqlStatementHelper
    {
        public static string ParseSqlStatement(
            string strQuery)
        {
            strQuery = strQuery.Replace("&lt;", "<");
            return strQuery;
        }

        public static string GetShrinkDbStatement(string strDbName)
        {
            return "DBCC SHRINKDATABASE (" + strDbName + ", 10);";
        }

        public static string GetDetachDbStatement(string strDbName)
        {
            return @"USE master EXEC sp_detach_db '" +
                   strDbName + "'";
        }

        public static string GetEnableClrStatement()
        {
            return "sp_configure 'clr enabled', 1 " +
                   "RECONFIGURE";
        }

        public static string GetRenameLogicalDbNameStatement(
            string strOldDbName,
            string strNewDbName)
        {
            return "ALTER DATABASE " + strOldDbName + " MODIFY FILE (NAME = '" +
                   strOldDbName + "', NEWNAME = '" + strNewDbName + "') " +
                   "ALTER DATABASE " + strOldDbName + " MODIFY FILE (NAME = '" +
                   Helper.GetDefaultLdfDbName(strOldDbName) +
                   "', NEWNAME = '" +
                   Helper.GetDefaultLdfDbName(strNewDbName) + "')";
        }

        public static string GetRmsVerStatement()
        {
            return "SELECT DBNUM FROM rmsver";
        }

        public static string GetExistDatabaseStatement(string strDbName)
        {
            return "USE master IF EXISTS(SELECT * FROM sys.databases WHERE NAME = '" +
                   strDbName + "')" +
                   " SELECT 1 " +
                   "ELSE " +
                   " SELECT 0";
        }

        public static string GetKillConnectionStatement(string strDbName)
        {
            return "USE master DECLARE @DatabaseName nvarchar(50) " +
                   " SET @DatabaseName = N'" + strDbName + "' " +
                   " DECLARE @SQL varchar(Max) " +
                   " SET @SQL = '' " +
                   " SELECT @SQL = @SQL + 'Kill ' + Convert(varchar, SPId) + ';' " +
                   " FROM MASTER..SysProcesses " +
                   " WHERE DBId = DB_ID(@DatabaseName) AND SPId <> @@SPId " +
                   " EXEC(@SQL) ";
        }

        public static string GetRenameDbNameStatement(
            string strOldDbName,
            string strNewDbName)
        {
            return GetKillConnectionStatement(strOldDbName) +
                   "EXEC sp_renamedb '" +
                   strOldDbName + "','" +
                   strNewDbName + "'";
        }


        public static string GetAttachDbStatement(
            string strDbName,
            string strPath)
        {
            return GetAttachDbStatement(
                strDbName,
                strPath,
                strPath);
        }

        public static string GetAttachDbStatement(
            string strDbName,
            string strRelativePath,
            string strNetworkPath)
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

            return GetAttachDbStatement(
                strDbName,
                strMdfNetworkFileName,
                strLdfNetworkFileName,
                strMdfRelativeFileName,
                strLdfRelativeFileName);
        }

        private static string GetAttachDbStatement2(
            string strDbName,
            string strRelativeMdfFileName)
        {
            // the following statement creates a log file in the same directory as the
            // mdf file
            return "CREATE DATABASE [" + strDbName + "] ON " +
                   " ( FILENAME = N'" + strRelativeMdfFileName + "' ) " +
                   " FOR ATTACH_REBUILD_LOG ";
        }

        //public static string GetAttachDbStatement(
        //    string strDbName,
        //    string strNetworkPath,
        //    string strMdfRelativeFileName,
        //    string strLdfRelativeFileName)
        //{
        //    string strLdfNetworkFileName =
        //        strNetworkPath + @"\" +
        //        new FileInfo(strLdfRelativeFileName).Name;
        //    string strMdfNetworkFileName =
        //        strNetworkPath + @"\" +
        //        new FileInfo(strMdfRelativeFileName).Name;

        //    return GetAttachDbStatement(
        //                strDbName,
        //                strLdfNetworkFileName,
        //                strMdfNetworkFileName,
        //                strMdfRelativeFileName,
        //                strLdfRelativeFileName);
        //}

        public static string GetAttachDbStatement(
            string strDbName,
            string strMdfNetworkFileName,
            string strLdfNetworkFileName,
            string strMdfRelativeFileName,
            string strLdfRelativeFileName)
        {
            if (FileHelper.Exists(strLdfNetworkFileName))
            {
                return @"EXEC sp_attach_db @dbname = N'" +
                       strDbName + @"', @filename1 = N'" +
                       strMdfRelativeFileName +
                       "', @filename2 = N'" +
                       strLdfRelativeFileName + "'";
            }
            else
            {
                return GetAttachDbStatement2(
                    strDbName,
                    strMdfRelativeFileName);
            }
        }


        public static string GetUseDbStatement(string strDbName)
        {
            return "USE " + strDbName;
        }

        public static string GetDbPhysicalFilesNamesStatement(
            string strDbName)
        {
            return "USE " + strDbName +
                   " SELECT filename FROM sysfiles";
        }

        public static string GetCreateDatabaseStatement(
            string strDbName,
            string strPath)
        {
            var strMdfFileName = strPath + @"\" +
                                 Helper.GetDefaultMdfFileName(strDbName);
            var strLdfFileName = strPath + @"\" +
                                 Helper.GetDefaultLdfFileName(strDbName);
            return GetCreateDatabaseStatement(
                strDbName,
                strMdfFileName,
                strLdfFileName);
        }

        public static string GetCreateDatabaseStatement(
            string strDbName)
        {
            return "CREATE DATABASE " + strDbName;
        }

        public static string GetCreateDatabaseStatement(
            string strDbName,
            string strMdfFileName,
            string strLdfFileName)
        {
            return "CREATE DATABASE " + strDbName +
                   " ON " +
                   " ( " +
                   "  NAME = " + strDbName + ", " +
                   "  FILENAME = N'" + strMdfFileName + "'  " +
                   " ) " +
                   " LOG ON " +
                   " (  " +
                   "  NAME = " + Helper.GetDefaultLdfDbName(strDbName) + ", " +
                   "  FILENAME = N'" + strLdfFileName + "'  " +
                   " )";
        }


        public static string GetDropTableStatement(
            string strTableName)
        {
            return "DROP TABLE " + strTableName;
        }

        public static string GetDropViewStatement(
            string strTableName)
        {
            return "IF OBJECT_ID ('" + strTableName + "', 'V') IS NOT NULL" +
                   " DROP VIEW '" + strTableName + "'";
        }

        public static string GetRenameTableStatement(
            string strOldTableName,
            string strNewTableName)
        {
            return "EXEC sp_rename '" + strOldTableName +
                   "', '" + strNewTableName + "'";
        }

        public static string GetRowCountStatement(string strTableName)
        {
            var queryBuilder = new SqlQueryBuilder();
            queryBuilder.AddField("COUNT(*)");
            queryBuilder.AddTable(strTableName);
            return queryBuilder.SelectQuery;
        }

        public static string GetDbList()
        {
            return "SELECT " +
                   "[name]  " +
                   "FROM " +
                   " sysdatabases ";
        }

        public static string GetSelectCountFromSqlStatement(
            string strSqlStatement)
        {
            return "SELECT COUNT(*) FROM (" +
                   strSqlStatement + ") AS tmp_1";
        }

        public static string GetSelectAllFromTableStatement(
            string strTableName)
        {
            var queryBuilder = new SqlQueryBuilder();
            queryBuilder.AddField("*");
            queryBuilder.AddTable(strTableName);
            return queryBuilder.SelectQuery;
        }

        public static string GetSelectTopFromTableStatement(
            string strTableName, int intRowCount)
        {
            var queryBuilder = new SqlQueryBuilder();
            queryBuilder.AddField(" TOP " + intRowCount);
            queryBuilder.AddTable(strTableName);
            return queryBuilder.SelectQuery;
        }

        public static string GetDeclareVariableStatement(
            string strVariableName,
            string strVariableType)
        {
            return "DECLARE " + strVariableName +
                   " " + strVariableType;
        }

        public static string GetBulkInsertSatement(
            string strPath,
            string strDatabaseName,
            string strTableName)
        {
            return "BULK INSERT " +
                   (strDatabaseName.Equals(string.Empty)
                        ? ""
                        : strDatabaseName + "..") +
                   strTableName + Environment.NewLine +
                   " FROM '" + strPath + "'" + Environment.NewLine +
                   " WITH " + Environment.NewLine +
                   " (" + Environment.NewLine +
                   " FIELDTERMINATOR = ','," + Environment.NewLine +
                   @" ROWTERMINATOR = '\n'" + Environment.NewLine +
                   " )";
        }

        public static string GetCreateViewStatement(
            string strStatement,
            string strViewName)
        {
            return "EXECUTE('" +
                   " CREATE VIEW " + strViewName +
                   " AS " + strStatement +
                   " ')";
        }

        public static string GetAddColumnStatement(
            string strTableName,
            Type columnType,
            string strColumnName)
        {
            var sb = new StringBuilder();

            sb.AppendLine("ALTER TABLE " + strTableName);
            sb.AppendLine("ADD ");

            AddColumnRow(
                columnType,
                sb,
                strColumnName);
            return sb.ToString();
        }

        public static string GetCreateTableStatement(
            string strTableName,
            List<Type> columnTypeList,
            List<string> columnNameList)
        {
            if (columnNameList.Count != columnTypeList.Count)
            {
                //Debugger.Break();
                throw new HCException("Error. Invalid column count.");
            }
            var sb = new StringBuilder();

            sb.AppendLine("CREATE TABLE " + strTableName);
            sb.AppendLine("(");

            AddColumnRow(
                columnTypeList[0],
                sb,
                columnNameList[0]);

            for (var i = 1; i < columnNameList.Count; i++)
            {
                sb.Append(",");
                AddColumnRow(
                    columnTypeList[i],
                    sb,
                    columnNameList[i]);
            }
            sb.AppendLine(")");

            return sb.ToString();
        }

        public static string GetWhereStatement(
            string strColumnName,
            object oColumnValue,
            InequalityType inequalityType,
            bool blnInCondition)
        {
            var strCondition = blnInCondition ? " IN " : MathHelper.GetInequalitySymbol(inequalityType);
            var strResult = strColumnName + strCondition;
            var type = oColumnValue.GetType();
            if (type == typeof (string))
            {
                return strResult += (string) oColumnValue;
            }
            else if (type == typeof (int))
            {
                return strResult += (int) oColumnValue;
            }
            else if (type == typeof (double))
            {
                return strResult += (double) oColumnValue;
            }
            else if (type == typeof (DateTime))
            {
                return strResult += (DateTime) oColumnValue;
            }
            else
            {
                throw new HCException("Error. Data type not defined.");
            }
        }

        private static void AddColumnRow(
            Type type,
            StringBuilder sb,
            string strColumnName)
        {
            if (type == typeof (string))
            {
                sb.AppendLine(strColumnName + " VARCHAR(1000)");
            }
            else if (type == typeof (int))
            {
                sb.AppendLine(strColumnName + " INT");
            }
            else if (type == typeof (double))
            {
                sb.AppendLine(strColumnName + " FLOAT");
            }
            else if (type == typeof (DateTime))
            {
                sb.AppendLine(strColumnName + " DATETIME");
            }
            else
            {
                throw new HCException("Error. Data type not defined.");
            }
        }

        public static string GetColumnsStatement(
            string strDbName,
            string strTableName)
        {
            return " EXECUTE " +
                   strDbName +
                   "..sp_columns [" +
                   strTableName + "]";
        }

        public static string DropGenericIndex(
            string strDbName,
            string strTableName)
        {
            return "DECLARE @idx_name VARCHAR(500) " +
                   "SET @idx_name = 'ix_' + '" + strTableName + "' + '_' + '" + strDbName + "' " +
                   "IF  EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('" +
                   strTableName + "') AND name = @idx_name) " +
                   "EXEC( " +
                   "'DROP INDEX ' + @idx_name  + '" +
                   " ON " + strDbName + ".." + strTableName + "  WITH ( ONLINE = OFF )')";
        }
    }
}
