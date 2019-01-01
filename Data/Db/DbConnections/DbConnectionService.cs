#region

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using HC.Core.Exceptions;
using HC.Core.Pooling;
using HC.Core.Resources;

#endregion

namespace HC.Utils.Basic.Data.Db.DbConnections
{
    [Serializable]
    public static class DbConnectionService
    {
        #region Constants

        private static readonly int POOL_CAPACITY = 1000000;

        #endregion

        #region Members

        private static readonly Dictionary<string, object> m_faultyConnStringDictionary =
            new Dictionary<string, object>();

        private static ResourcePool m_resourcePool;

        #endregion

        public static DbConnectionWrapper Reserve(
            string serverName,
            string databaseName,
            string strConnectionString,
            DbProviderType dbProviderType)
        {
            if (m_resourcePool == null)
            {
                InitializeResourcePool(
                    dbProviderType);
            }
            else
            {
                // set data resource type
                ((DbConnectionFactory) m_resourcePool.Factory).SetDataProviderType(
                    dbProviderType);
            }

            if (m_faultyConnStringDictionary.ContainsKey(strConnectionString))
            {
                throw new HCException("Error. Bad db connection.");
            }

            var sqlConnectionStringBuilder =
                new SqlConnectionStringBuilder(
                    strConnectionString);

            var dbDataRequest =
                new DbDataRequest(
                    sqlConnectionStringBuilder.DataSource,
                    sqlConnectionStringBuilder.InitialCatalog);

            var connection = m_resourcePool.Reserve(null,
                                                    dbDataRequest) as DbConnectionWrapper;

            if (!databaseName.Equals(string.Empty))
            {
                connection.ChangeDatabase(databaseName);
            }

            return connection;
        }

        public static void Release(DbConnectionWrapper connection)
        {
            m_resourcePool.Release(connection);
        }

        public static readonly object m_resourceLockObject =
            new object();

        public static void AddServerToResourcePool(
            string strServerName,
            string strDbName,
            string strConnectionString,
            DbProviderType dbProviderType)
        {
            if (m_resourcePool == null)
            {
                lock (m_resourceLockObject)
                {
                    if (m_resourcePool == null)
                    {
                        InitializeResourcePool(
                            dbProviderType);
                    }
                }
            }

            var connectionFactory = (DbConnectionFactory) m_resourcePool.Factory;

            //
            // set new connection info
            //
            var blnIsNewConnection = connectionFactory.SetConnectionInfo(
                strServerName,
                strConnectionString);

            try
            {
                //
                // test new db resource
                //
                var conn = Reserve(
                    strServerName,
                    strDbName,
                    strConnectionString,
                    dbProviderType);
                Release(conn);
            }
            catch (Exception e)
            {
                //Debugger.Break();
                if (blnIsNewConnection)
                {
                    // remove connection info
                    connectionFactory.RemoveConnectionInfo(
                        strConnectionString);
                    // add the connection string to a black list
                    m_faultyConnStringDictionary.Add(
                        strConnectionString, null);
                }
                throw;
            }
        }

        private static void InitializeResourcePool(
            DbProviderType dbProviderType)
        {
            // create a resource pool if it doesn't exist existing pools 
            m_resourcePool = ResourcePool.GetInstance(typeof (DbConnectionFactory));
            if (m_resourcePool == null)
            {
                var connectionFactory = new DbConnectionFactory(
                    dbProviderType);
                m_resourcePool = ResourcePool.CreateInstance(
                    connectionFactory,
                    GetPoolName(),
                    false,
                    POOL_CAPACITY);
            }
        }

        public static string GetPoolName()
        {
            return "Database Connection Pool";
        }
    }
}
