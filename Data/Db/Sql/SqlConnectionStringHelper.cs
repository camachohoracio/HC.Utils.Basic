#region

using System.Data.SqlClient;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public static class SqlConnectionStringHelper
    {
        public static SqlConnectionStringBuilder GetTrustedConnectionStringBuilder(
            string strServerName)
        {
            return GetTrustedConnectionStringBuilder(
                strServerName,
                "master");
        }

        public static SqlConnectionStringBuilder GetTrustedConnectionStringBuilder(
            string strServerName,
            string strDatabaseName)
        {
            var connectionString =
                new SqlConnectionStringBuilder(
                    "Initial Catalog=MyDb;Data Source=MyServer;Integrated Security=SSPI;persist security info=False;Trusted_Connection=Yes");
            connectionString.DataSource = strServerName;
            connectionString.InitialCatalog = strDatabaseName;
            connectionString.IntegratedSecurity = true;

            return connectionString;
        }
    }
}
