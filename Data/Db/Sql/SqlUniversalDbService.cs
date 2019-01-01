#region

using System.Collections.Generic;
using HC.Utils.Basic.Data.Db.Parameters;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public class SqlUniversalDbService : AbstractDataService
    {
        #region Constructors

        public SqlUniversalDbService(
            string strServerName,
            string strDbName,
            string strConnectionString)
            : base(
                strServerName,
                strDbName,
                strConnectionString,
                DbProviderType.SQL)
        {
        }

        #endregion

        public DbDataReaderWrapper GetDataReader(
            string strDbName,
            List<string> strTableList,
            List<string> columnList,
            List<string> whereColumnList,
            List<object> whereValuesList,
            List<string> groupByList,
            List<string> orderByList)
        {
            var sqlQueryBuilder = new SqlQueryBuilder
                                      {
                                          TableList = strTableList,
                                          ColumnList = columnList,
                                          WhereList = whereColumnList,
                                          GroupByList = groupByList,
                                          DbName = strDbName,
                                          OrderByList = orderByList
                                      };
            var strQuery = sqlQueryBuilder.SelectQuery;
            var inputParameters = GetInputParametersFromList(
                whereColumnList,
                whereValuesList);
            return ExecuteReader(
                strQuery,
                inputParameters);
        }

        public int GetRowCount(
            string strDbName,
            List<string> strTableList,
            List<string> whereColumnList,
            List<object> whereValuesList)
        {
            var sqlQueryBuilder = new SqlQueryBuilder
                                      {
                                          TableList = strTableList,
                                          WhereList = whereColumnList,
                                          DbName = strDbName,
                                      };
            var strQuery = sqlQueryBuilder.SelectCountQuery;
            var inputParameters = GetInputParametersFromList(
                whereColumnList,
                whereValuesList);
            return SelectValue<int>(
                strQuery,
                inputParameters);
        }


        private List<IDbParameter> GetInputParametersFromList(
            List<string> whereColumnList,
            List<object> whereValuesList)
        {
            //
            // get input parameters
            //
            var inputParameters = new List<IDbParameter>();

            if (whereColumnList != null)
            {
                for (var i = 0; i < whereValuesList.Count; i++)
                {
                    inputParameters.Add(
                        m_parameterFactory.BuildInputParameter(
                            "@" + whereColumnList[i],
                            whereValuesList[i]));
                }
            }
            return inputParameters;
        }

        public DbDataReaderWrapper GetDataReader(
            string strDbName,
            string strTableName,
            List<string> columnList,
            List<string> groupByList)
        {
            var tableList = new List<string>();
            tableList.Add(strTableName);
            var sqlQueryBuilder = new SqlQueryBuilder
                                      {
                                          TableList = tableList,
                                          ColumnList = columnList,
                                          GroupByList = groupByList,
                                          DbName = strDbName
                                      };
            var strQuery = sqlQueryBuilder.SelectQuery;
            return ExecuteReader(
                strQuery);
        }
    }
}
