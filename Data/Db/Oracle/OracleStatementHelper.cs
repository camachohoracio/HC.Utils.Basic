#region

using System;
using System.Collections.Generic;
using System.Text;
using HC.Core.Exceptions;

#endregion

namespace HC.Utils.Basic.Data.Db.Oracle
{
    public class OracleStatementHelper
    {
        public static string SelectAllTableNames()
        {
            return "SELECT table_name FROM all_tables";
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


        private static void AddColumnRow(
            Type type,
            StringBuilder sb,
            string strColumnName)
        {
            if (type == typeof (string))
            {
                sb.AppendLine(strColumnName + " VARCHAR2(1000)");
            }
            else if (type == typeof (int))
            {
                sb.AppendLine(strColumnName + " NUMBER");
            }
            else if (type == typeof (double))
            {
                sb.AppendLine(strColumnName + " NUMBER");
            }
            else if (type == typeof (DateTime))
            {
                sb.AppendLine(strColumnName + " DATE");
            }
            else
            {
                throw new HCException("Error. Data type not defined.");
            }
        }
    }
}
