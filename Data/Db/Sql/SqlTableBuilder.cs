#region

using System;
using System.Collections.Generic;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public class SqlTableBuilder
    {
        private readonly string m_strTableName;
        private readonly List<string> m_variableList;

        public SqlTableBuilder(string strTableName)
        {
            m_strTableName = strTableName;
            m_variableList = new List<string>();
        }

        public string SqlCreateTable
        {
            get
            {
                return "CREATE TABLE " + m_strTableName +
                       Environment.NewLine + "(" +
                       Environment.NewLine + GetVariableList() +
                       Environment.NewLine + ")";
            }
        }

        public string SqlTruncateTable
        {
            get { return "TRUNCATE TABLE " + m_strTableName; }
        }

        public string SqlDropTable
        {
            get
            {
                return "BEGIN TRY" + Environment.NewLine +
                       " DROP TABLE " + m_strTableName +
                       Environment.NewLine +
                       "END TRY" + Environment.NewLine +
                       "BEGIN CATCH" + Environment.NewLine +
                       "END CATCH";
            }
        }

        public void AddVariable(
            string strVariableName,
            string strVariableType)
        {
            m_variableList.Add(
                strVariableName + " " +
                strVariableType);
        }

        private string GetVariableList()
        {
            return Helper.GetItemList(m_variableList);
        }
    }
}
