#region

using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient;
using System.Data.SqlClient;
using HC.Core.Exceptions;
using HC.Utils.Basic.Data.Db.Parameters;
using lcpi.data.oledb;

#endregion

namespace HC.Utils.Basic.Data.Db.DbConnections
{
    public static class DbCommandFactory
    {
        public static DbCommand BuildDbCommand(
            DbProviderType dbProviderType,
            string strCommandText,
            DbConnection dbConnection)
        {
            return BuildDbCommand(
                dbProviderType,
                strCommandText,
                dbConnection,
                false,
                null);
        }


        public static DbCommand BuildDbCommand(
            DbProviderType dbProviderType,
            string strCommandText,
            DbConnection dbConnection,
            bool blnTransaction)
        {
            return BuildDbCommand(
                dbProviderType,
                strCommandText,
                dbConnection,
                blnTransaction,
                null);
        }

        public static DbCommand BuildDbCommand(
            DbProviderType dbProviderType,
            string strCommandText,
            DbConnection dbConnection,
            List<IDbParameter> inputParameters)
        {
            return BuildDbCommand(
                dbProviderType,
                strCommandText,
                dbConnection,
                false,
                inputParameters);
        }

        public static DbCommand BuildDbCommand(
            DbProviderType dbProviderType,
            string strCommandText,
            DbConnection dbConnection,
            bool blnTransaction,
            List<IDbParameter> inputParameters)
        {
            DbCommand dbCommand = null;
            switch (dbProviderType)
            {
                case DbProviderType.SQL:
                    {
                        dbCommand = new SqlCommand();
                        break;
                    }
                case DbProviderType.ORACLE:
                    {
                        dbCommand = new OracleCommand();
                        break;
                    }
                case DbProviderType.OTHER:
                    {
                        dbCommand = new OleDbCommand();
                        break;
                    }
                default:
                    throw new HCException("Connection type not defined");
            }

            // set command settings
            dbCommand.CommandText = strCommandText;
            dbCommand.CommandTimeout = 0;
            dbCommand.CommandType = CommandType.Text;
            dbCommand.Connection = dbConnection;

            if (blnTransaction)
            {
                var transaction = dbConnection.BeginTransaction();
                dbCommand.Transaction = transaction;
            }

            //
            // set input parameters
            //
            if (inputParameters != null)
            {
                foreach (IDbParameter dbInputParameter in inputParameters)
                {
                    dbCommand.Parameters.Add(
                        DbParameterFactory.BuildDbParameter(
                            dbInputParameter,
                            dbProviderType));
                }
            }

            return dbCommand;
        }
    }
}
