#region

using System.Data;
using System.Data.OracleClient;
using System.Data.SqlClient;
using HC.Core.Exceptions;

#endregion

namespace HC.Utils.Basic.Data.Db.Parameters
{
    public class DbParameterFactory
    {
        #region Members

        private readonly DbProviderType m_dbProviderType;

        #endregion

        #region Constructors

        public DbParameterFactory(DbProviderType dbProviderType)
        {
            m_dbProviderType = dbProviderType;
        }

        #endregion

        public IDbParameter BuildInputParameter(
            string strName,
            object value)
        {
            var intDbTypeId =
                Helper.GetDbTypeId(value.GetType());

            return BuildInputParameter(
                strName,
                value,
                intDbTypeId);
        }

        public IDbParameter BuildInputParameter(
            string strName,
            object value,
            int intDbTypeId)
        {
            return new DbInputParameter(
                strName,
                value,
                m_dbProviderType,
                intDbTypeId);
        }

        public static object BuildDbParameter(
            IDbParameter dbParameter,
            DbProviderType dbProviderType)
        {
            switch (dbProviderType)
            {
                case DbProviderType.SQL:
                    {
                        var parameter = new SqlParameter();
                        {
                            parameter.ParameterName = dbParameter.Name;
                            parameter.Direction = dbParameter.ParameterDirection;
                            parameter.SqlDbType = (SqlDbType) dbParameter.DbTypeId;
                            parameter.Value = dbParameter.Value;
                        }
                        return parameter;
                    }
                case DbProviderType.ORACLE:
                    {
                        var parameter = new OracleParameter();
                        {
                            parameter.ParameterName = dbParameter.Name;
                            parameter.Direction = dbParameter.ParameterDirection;
                            parameter.Value = dbParameter.Value;
                            parameter.OracleType =
                                Helper.GetOracleDbType(dbParameter.Value);
                        }
                        return parameter;
                    }
            }


            throw new HCException("Error. Db provider type not defined.");
        }
    }
}
