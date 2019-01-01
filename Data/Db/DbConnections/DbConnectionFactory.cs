#region

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OracleClient;
using System.Data.SqlClient;
using HC.Core.Exceptions;
using HC.Core.Logging;
using HC.Core.Resources;
using lcpi.data.oledb;

#endregion

namespace HC.Utils.Basic.Data.Db.DbConnections
{
    [Serializable]
    public class DbConnectionFactory : IResourceFactory
    {
        #region Members

        private static readonly ILoggerService m_lc = Logger.GetLogger();
        private IDictionary<IDataRequest, string> m_connectionStrings;
        //private static DbConnectionFactory GetPublisher = null;
        private DbProviderType m_dbProviderType;

        #endregion

        #region Properties

        public string Name { get; set; }

        #endregion

        #region Constructors

        public DbConnectionFactory(DbProviderType dbProviderType)
        {
            Initialize(dbProviderType);
        }

        #endregion

        #region Initializers

        private void Initialize(DbProviderType dbProviderType)
        {
            m_dbProviderType = dbProviderType;
            m_connectionStrings = new Dictionary<IDataRequest, string>(
                new PoolDataComparer());
        }

        #endregion

        #region Public

        public virtual bool MultipleInstances()
        {
            return true;
        }

        public virtual bool Shared()
        {
            return false;
        }

        public virtual string[] Resources()
        {
            return null;
        }

        public virtual IResource Create(
            IDataRequest serverName)
        {
            DbConnectionWrapper resource = null;
            if (m_connectionStrings.ContainsKey(serverName))
            {
                try
                {
                    resource = new DbConnectionWrapper(
                        m_dbProviderType,
                        m_connectionStrings[serverName]);

                    resource.DataRequest = serverName;
                }
                catch (Exception e)
                {
                    m_lc.Write("Error in creating database connection.");
                    m_lc.Write("serverName Name : " + serverName);
                    m_lc.Write("Connection String : " + m_connectionStrings[serverName]);
                    m_lc.Write(e);
                    throw;
                }
            }
            return resource;
        }

        private readonly object m_connectionInfoLock = new object();

        public bool SetConnectionInfo(
            string serverName,
            string strConnectionString)
        {
            var connectionStringBuilder =
                new SqlConnectionStringBuilder(strConnectionString);
            var dbDataRequest =
                new DbDataRequest(
                    serverName,
                    connectionStringBuilder.InitialCatalog);

            if (!m_connectionStrings.ContainsKey(dbDataRequest))
            {
                lock (m_connectionInfoLock)
                {
                    if (!m_connectionStrings.ContainsKey(dbDataRequest))
                    {
                        m_connectionStrings.Add(dbDataRequest, strConnectionString);
                        return true;
                    }
                }
            }
            return false;
        }

        public void RemoveConnectionInfo(
            string strConnectionString)
        {
            var sqlConnectionStringBuilder =
                new SqlConnectionStringBuilder(
                    strConnectionString);

            var dbDataRequest =
                new DbDataRequest(
                    sqlConnectionStringBuilder.DataSource,
                    sqlConnectionStringBuilder.InitialCatalog);

            if (m_connectionStrings.ContainsKey(dbDataRequest))
            {
                m_connectionStrings.Remove(dbDataRequest);
            }
        }

        public static IDbConnection BuildConnection(
            DbProviderType dbProviderType,
            string strConnectionString)
        {
            IDbConnection conn = null;
            switch (dbProviderType)
            {
                case DbProviderType.SQL:
                    {
                        conn = new SqlConnection(strConnectionString);
                        break;
                    }
                case DbProviderType.ORACLE:
                    {
                        conn = new OracleConnection(strConnectionString);
                        break;
                    }
                case DbProviderType.OTHER:
                    {
                        conn = new OleDbConnection(strConnectionString);
                        break;
                    }
                default:
                    throw new HCException("Connection type not defined");
            }
            // open the connection
            conn.Open();
            return conn;
        }

        public void SetDataProviderType(DbProviderType dbProviderType)
        {
            m_dbProviderType = dbProviderType;
        }

        #endregion
    }
}
