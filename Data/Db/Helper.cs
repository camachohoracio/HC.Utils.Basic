#region

using System;
using System.Data;
using System.Data.OracleClient;
using System.Data.SqlClient;
using HC.Core.Exceptions;

#endregion

namespace HC.Utils.Basic.Data.Db
{
    public static class Helper
    {
        public static string GetConnectionName(
            string strConnectionString)
        {
            var connectionString2 =
                new SqlConnectionStringBuilder(strConnectionString);
            //string strKey = connectionString2.DataSource;
            //string strKey =
            //    connectionString2.DataSource + "_" +
            //    connectionString2.UserID + "_" +
            //    connectionString2.Password;
            var strKey =
                connectionString2.DataSource + "_" +
                connectionString2.InitialCatalog;
            return strKey;
        }

        public static int GetDbTypeId(Type type)
        {
            if (type.Equals(typeof (int)))
            {
                return Convert.ToInt32(SqlDbType.Int);
            }

            if (type.Equals(typeof (double)))
            {
                return Convert.ToInt32(SqlDbType.Float);
            }

            if (type.Equals(typeof (DateTime)))
            {
                return Convert.ToInt32(SqlDbType.DateTime);
            }

            if (type.Equals(typeof (string)))
            {
                return Convert.ToInt32(SqlDbType.VarChar);
            }

            if (type.Equals(typeof (bool)))
            {
                return Convert.ToInt32(SqlDbType.Bit);
            }

            if (type == null)
            {
                return Convert.ToInt32(SqlDbType.Int);
            }

            if (type.Equals(DBNull.Value))
            {
                return Convert.ToInt32(SqlDbType.Int);
            }

            throw new HCException("DB type not found.");
        }


        public static OracleType GetOracleDbType(object o)
        {
            var type = o.GetType();

            if (type.Equals(typeof (int)))
            {
                return OracleType.Int32;
            }

            if (type.Equals(typeof (double)))
            {
                return OracleType.Number;
            }

            if (type.Equals(typeof (DateTime)))
            {
                return OracleType.DateTime;
            }

            if (type.Equals(typeof (string)))
            {
                return OracleType.VarChar;
            }

            if (type.Equals(typeof (bool)))
            {
                return OracleType.Byte;
            }

            if (type == null)
            {
                return OracleType.Int32;
            }

            if (type.Equals(DBNull.Value))
            {
                return OracleType.Int32;
            }

            throw new HCException("DB type not found.");
        }
    }
}
