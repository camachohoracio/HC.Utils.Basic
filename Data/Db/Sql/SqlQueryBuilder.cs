#region

using System;
using System.Collections.Generic;
using System.Text;
using HC.Analytics.Mathematics;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    /// <summary>
    ///   Builds SQL queries from lists of columns, constraints and order by
    /// </summary>
    public class SqlQueryBuilder
    {
        public List<string> ColumnList { get; set; }
        public List<string> GroupByList { get; set; }
        public List<string> OrderByList { get; set; }
        public List<string> TableList { get; set; }
        public List<string> WhereList { get; set; }
        public List<string> VariableList { get; set; }
        public string DbName { get; set; }

        public string SelectQuery
        {
            get
            {
                return (DbName != null && !DbName.Equals(string.Empty)
                            ? "USE " + DbName + Environment.NewLine
                            : "") +
                       "SELECT " + Environment.NewLine +
                       (ColumnList != null && ColumnList.Count > 0 ? GetFieldList() : "*") +
                       Environment.NewLine +
                       "FROM" + Environment.NewLine + GetTableList() +
                       Environment.NewLine +
                       (WhereList != null && WhereList.Count > 0
                            ? "WHERE" + Environment.NewLine + GetWhereList()
                            : "") +
                       (GroupByList != null && GroupByList.Count > 0
                            ? Environment.NewLine + "GROUP BY" + Environment.NewLine + GetGroupByList()
                            : "") +
                       (OrderByList != null && OrderByList.Count > 0
                            ? Environment.NewLine + "ORDER BY" + Environment.NewLine + GetOrderByList()
                            : "");
            }
        }

        public string SelectCountQuery
        {
            get
            {
                return (DbName != null && !DbName.Equals(string.Empty)
                            ? "USE " + DbName + Environment.NewLine
                            : "") +
                       "SELECT COUNT(*)" +
                       Environment.NewLine +
                       "FROM" + Environment.NewLine + GetTableList() +
                       Environment.NewLine +
                       (WhereList != null && WhereList.Count > 0
                            ? "WHERE" + Environment.NewLine + GetWhereList()
                            : "");
            }
        }

        public string SelectDistinctQuery
        {
            get
            {
                return
                    (DbName != null && !DbName.Equals(string.Empty)
                         ? "USE " + DbName + Environment.NewLine
                         : "") +
                    "SELECT DISTINCT " + Environment.NewLine +
                    (ColumnList != null && ColumnList.Count > 0 ? GetFieldList() : "*") +
                    Environment.NewLine +
                    "FROM" + Environment.NewLine +
                    GetTableList() + Environment.NewLine +
                    (WhereList != null && WhereList.Count > 0
                         ? "WHERE" + Environment.NewLine + GetWhereList()
                         : "") +
                    (GroupByList != null && GroupByList.Count > 0
                         ? "GROUP BY" + Environment.NewLine + GetGroupByList()
                         : "") +
                    (OrderByList != null && OrderByList.Count > 0
                         ? "ORDER BY" + Environment.NewLine + GetOrderByList()
                         : "");
            }
        }

        #region Constructors

        public SqlQueryBuilder(
            List<string> columnList,
            List<string> tableList,
            List<string> whereList,
            List<string> orderByList,
            List<string> groupByList,
            string strDbName)
        {
            ColumnList = columnList;
            TableList = tableList;
            WhereList = whereList;
            OrderByList = orderByList;
            GroupByList = groupByList;
            DbName = strDbName;
        }


        public SqlQueryBuilder() : this(
            new List<string>(),
            new List<string>(),
            new List<string>(),
            new List<string>(),
            new List<string>(),
            "")
        {
        }

        #endregion

        public void AddTable(string strTableName)
        {
            TableList.Add(strTableName);
        }

        public void AddField(string strColumnName,
                             string strVariableName)
        {
            ColumnList.Add(
                strVariableName + " = " +
                strColumnName);
        }

        public void AddField(string strColumnName)
        {
            ColumnList.Add(strColumnName);
        }

        public void AddWhere(
            string strColumnName,
            object oColumnValue,
            InequalityType inequalityType,
            bool blnInCondition)
        {
            WhereList.Add(
                SqlStatementHelper.GetWhereStatement(
                    strColumnName,
                    oColumnValue,
                    inequalityType,
                    blnInCondition));
        }

        public void AddOrderBy(string strFieldName)
        {
            OrderByList.Add(strFieldName);
        }

        public void AddGroupBy(string strFieldName)
        {
            GroupByList.Add(strFieldName);
        }

        private string GetTableList()
        {
            return Helper.GetItemList(TableList);
        }

        private string GetFieldList()
        {
            return Helper.GetItemList(ColumnList);
        }

        private string GetOrderByList()
        {
            return Helper.GetItemList(OrderByList);
        }

        private string GetGroupByList()
        {
            return Helper.GetItemList(GroupByList);
        }

        private string GetWhereList()
        {
            if (WhereList.Count == 0)
            {
                return "";
            }
            var sb = new StringBuilder();
            sb.Append(WhereList[0]);
            for (var i = 1; i < WhereList.Count; i++)
            {
                sb.Append("\nAND " + WhereList[i]);
            }
            return sb.ToString();
        }
    }
}
