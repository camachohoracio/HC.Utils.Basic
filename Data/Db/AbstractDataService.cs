#region

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using HC.Core.Logging;
using HC.Core.Pooling;
using HC.Core.Resources;
using HC.Utils.Basic.Data.Db.DbConnections;
using HC.Utils.Basic.Data.Db.Parameters;

#endregion

namespace HC.Utils.Basic.Data.Db
{
    public abstract class AbstractDataService : IDisposable
    {
        #region Properties

        public string ConnectionString { get; set; }

        public SqlConnectionStringBuilder ConnectionStringBuilder
        {
            get { return new SqlConnectionStringBuilder(ConnectionString); }
        }

        #endregion

        #region Members

        private static readonly ILoggerService m_lc = Logger.GetLogger();
        private readonly DbProviderType m_dbProviderType;
        private readonly string m_strDbName;

        protected DbConnectionWrapper m_dbConnection;
        protected DbParameterFactory m_parameterFactory;
        private ResourcePool m_resoucePool;

        #endregion

        #region Constructors

        public AbstractDataService(){}

        /// <summary>
        ///   Constructor
        /// </summary>
        /// <param name = "strServerName"></param>
        /// <param name = "strDbName"></param>
        /// <param name="strConnectionString"></param>
        /// <param name="dbProviderType"></param>
        public AbstractDataService(
            string strServerName,
            string strDbName,
            string strConnectionString,
            DbProviderType dbProviderType)
        {
            try
            {
                ConnectionString = strConnectionString;
                m_strDbName = strDbName;
                m_dbProviderType = dbProviderType;

                //
                // register server to resource pool
                //
                DbConnectionService.AddServerToResourcePool(
                    strServerName,
                    strDbName,
                    strConnectionString,
                    dbProviderType);

                //
                // create a new paramater factory
                //
                m_parameterFactory = new DbParameterFactory(
                    m_dbProviderType);

                Logger.Log("Loaded data service [" + GetType().Name + "]");
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
        }

        #endregion

        /// <summary>
        ///   Dispose flag
        /// </summary>
        private bool m_disposed;

        #region IDisposable Members

        /// <summary>
        ///   Dispose class
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        #endregion

        protected DbConnectionWrapper Reserve()
        {
            if (m_resoucePool == null)
            {
                m_resoucePool = ResourcePool.GetInstance(typeof (DbConnectionFactory));
            }
            var strConnectionName =
                Helper.GetConnectionName(ConnectionString);

            var connectionStringBuilder =
                new SqlConnectionStringBuilder(ConnectionString);

            var dbDataRequest =
                new DbDataRequest(
                    connectionStringBuilder.DataSource,
                    connectionStringBuilder.InitialCatalog);

            var connection =
                (DbConnectionWrapper)m_resoucePool.Reserve(this,
                                      dbDataRequest);

            if (!m_strDbName.Equals(string.Empty))
            {
                connection.ChangeDatabase(m_strDbName);
            }

            return connection;
        }

        protected void Release(DbConnectionWrapper connection)
        {
            m_resoucePool.Release(connection);
        }

        protected DbDataReaderWrapper ExecuteReader(
            string strQuery)
        {
            return ExecuteReader(
                strQuery,
                null);
        }

        protected DbDataReaderWrapper ExecuteReader(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            // this method does not release the connection
            // connection will be released once the reader is closed
            DbConnectionWrapper conn = null;
            try
            {
                //take connection
                conn = Reserve();
                return conn.ExecuteReader(
                    strQuery,
                    inputParameters);
            }
            catch (Exception e)
            {
                m_lc.Write("Error executing select into data table.");
                m_lc.Write(e);
                m_lc.Write("Sql: " + strQuery);
                if (inputParameters != null)
                {
                    m_lc.Write("Parameters");

                    foreach (DbInputParameter dbInputParameter in inputParameters)
                    {
                        m_lc.Write(dbInputParameter.Name + "=" +
                                   dbInputParameter.Value);
                    }
                }
                throw;
            }
        }


        protected int ExecuteNonQuery(
            string strQuery)
        {
            return ExecuteNonQuery(
                strQuery,
                null,
                true);
        }

        protected int ExecuteNonQuery(
            string strQuery,
            bool blnTrans)
        {
            return ExecuteNonQuery(
                strQuery,
                null,
                blnTrans);
        }

        protected int ExecuteNonQuery(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            return ExecuteNonQuery(
                strQuery,
                inputParameters,
                true);
        }

        protected int ExecuteNonQuery(
            string strQuery,
            List<IDbParameter> inputParameters,
            bool blnTrans)
        {
            return ExecuteNonQuery(
                strQuery,
                inputParameters,
                blnTrans,
                "");
        }

        public void SwapConnectionToDatabase(
            string strDbName)
        {
            var strQuery = "USE " + strDbName;
            ExecuteNonQuery(
                strQuery);
        }


        protected int ExecuteNonQuery(
            string strQuery,
            List<IDbParameter> inputParameters,
            bool blnTrans,
            string strDbSwap)
        {
            DbConnectionWrapper conn = null;
            try
            {
                //take connection
                conn = Reserve();

                //
                // check if swap of connection is needed
                //
                if (!strDbSwap.Equals(string.Empty))
                {
                    var strSwapQuery = "USE " + strDbSwap;
                    conn.ExecuteNonQuery(
                        strSwapQuery,
                        null,
                        false);
                }

                return conn.ExecuteNonQuery(
                    strQuery,
                    inputParameters,
                    blnTrans);
            }
            catch (Exception e)
            {
                m_lc.Write("Error executing select into data table.");
                m_lc.Write(e);
                m_lc.Write("Sql: " + strQuery);
                if (inputParameters != null)
                {
                    m_lc.Write("Parameters");

                    foreach (DbInputParameter dbInputParameter in inputParameters)
                    {
                        m_lc.Write(dbInputParameter.Name + "=" +
                                   dbInputParameter.Value);
                    }
                }
                throw;
            }
            finally
            {
                //Release connection to pool
                if (conn != null)
                {
                    Release(conn);
                }
            }
        }

        protected int Insert(
            string strQuery)
        {
            return Insert(
                strQuery,
                null,
                true);
        }

        protected int Insert(
            string strQuery,
            bool blnTrans)
        {
            return Insert(
                strQuery,
                null,
                blnTrans);
        }

        protected int Insert(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            return Insert(
                strQuery,
                inputParameters,
                true);
        }

        protected int Insert(
            string strQuery,
            List<IDbParameter> inputParameters,
            bool blnTrans)
        {
            DbConnectionWrapper conn = null;
            try
            {
                //take connection
                conn = Reserve();
                return conn.Insert(
                    strQuery,
                    inputParameters,
                    blnTrans);
            }
            catch (Exception e)
            {
                m_lc.Write("Error executing select into data table.");
                m_lc.Write(e);
                m_lc.Write("Sql: " + strQuery);
                if (inputParameters != null)
                {
                    m_lc.Write("Parameters");

                    foreach (DbInputParameter dbInputParameter in inputParameters)
                    {
                        m_lc.Write(dbInputParameter.Name + "=" +
                                   dbInputParameter.Value);
                    }
                }
                throw;
            }
            finally
            {
                //Release connection to pool
                if (conn != null)
                {
                    Release(conn);
                }
            }
        }

        protected int Delete(
            string strQuery)
        {
            return Delete(
                strQuery,
                null,
                true);
        }

        protected int Delete(
            string strQuery,
            bool blnTans)
        {
            return Delete(
                strQuery,
                null,
                blnTans);
        }

        protected int Delete(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            return Delete(
                strQuery,
                inputParameters,
                true);
        }

        protected int Delete(
            string strQuery,
            List<IDbParameter> inputParameters,
            bool blnTrans)
        {
            DbConnectionWrapper conn = null;
            try
            {
                //take connection
                conn = Reserve();
                return conn.Delete(
                    strQuery,
                    inputParameters,
                    blnTrans);
            }
            catch (Exception e)
            {
                m_lc.Write("Error executing select into data table.");
                m_lc.Write(e);
                m_lc.Write("Sql: " + strQuery);
                if (inputParameters != null)
                {
                    m_lc.Write("Parameters");

                    foreach (DbInputParameter dbInputParameter in inputParameters)
                    {
                        m_lc.Write(dbInputParameter.Name + "=" +
                                   dbInputParameter.Value);
                    }
                }
                throw;
            }
            finally
            {
                //Release connection to pool
                if (conn != null)
                {
                    Release(conn);
                }
            }
        }

        protected int Update(
            string strQuery)
        {
            return Update(
                strQuery,
                null,
                true);
        }

        protected int Update(
            string strQuery,
            bool blnTrans)
        {
            return Update(
                strQuery,
                null,
                blnTrans);
        }

        protected int Update(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            return Update(
                strQuery,
                inputParameters,
                true);
        }

        protected int Update(
            string strQuery,
            List<IDbParameter> inputParameters,
            bool blnTrans)
        {
            DbConnectionWrapper conn = null;
            try
            {
                //take connection
                conn = Reserve();
                return conn.Update(
                    strQuery,
                    inputParameters,
                    blnTrans);
            }
            catch (Exception e)
            {
                m_lc.Write("Error executing select into data table.");
                m_lc.Write(e);
                m_lc.Write("Sql: " + strQuery);
                if (inputParameters != null)
                {
                    m_lc.Write("Parameters");

                    foreach (DbInputParameter dbInputParameter in inputParameters)
                    {
                        m_lc.Write(dbInputParameter.Name + "=" +
                                   dbInputParameter.Value);
                    }
                }
                throw;
            }
            finally
            {
                //Release connection to pool
                if (conn != null)
                {
                    Release(conn);
                }
            }
        }

        protected void SelectInto(
            string strQuery,
            DataTable dt)
        {
            SelectInto(strQuery,
                       null,
                       dt);
        }

        public void SelectInto(
            string strQuery,
            List<IDbParameter> inputParameters,
            DataTable dt)
        {
            DbConnectionWrapper conn = null;
            try
            {
                //take connection
                conn = Reserve();
                conn.SelectInto(
                    strQuery,
                    inputParameters,
                    dt);
            }
            catch (Exception e)
            {
                m_lc.Write("Error executing select into data table.");
                m_lc.Write(e);
                m_lc.Write("Sql: " + strQuery);
                if (inputParameters != null)
                {
                    m_lc.Write("Parameters");

                    foreach (DbInputParameter dbInputParameter in inputParameters)
                    {
                        m_lc.Write(dbInputParameter.Name + "=" +
                                   dbInputParameter.Value);
                    }
                }
                throw;
            }
            finally
            {
                //Release connection to pool
                if (conn != null)
                {
                    Release(conn);
                }
            }
        }

        protected T SelectValue<T>(
            string strQuery)
        {
            return SelectValue<T>(
                strQuery,
                null);
        }

        protected T SelectValue<T>(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            DbConnectionWrapper conn = null;
            try
            {
                //take connection
                conn = Reserve();
                return conn.SelectValue<T>(
                    strQuery,
                    inputParameters);
            }
            catch (Exception e)
            {
                m_lc.Write("Error executing select into data table.");
                m_lc.Write(e);
                m_lc.Write("Sql: " + strQuery);
                if (inputParameters != null)
                {
                    m_lc.Write("Parameters");

                    foreach (DbInputParameter dbInputParameter in inputParameters)
                    {
                        m_lc.Write(dbInputParameter.Name + "=" +
                                   dbInputParameter.Value);
                    }
                }
                throw;
            }
            finally
            {
                //Release connection to pool
                if (conn != null)
                {
                    Release(conn);
                }
            }
        }


        protected List<T> SelectValueList<T>(
            string strQuery)
        {
            return SelectValueList<T>(
                strQuery,
                null);
        }

        protected List<T> SelectValueList<T>(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            DbConnectionWrapper conn = null;
            try
            {
                //take connection
                conn = Reserve();
                return conn.SelectValueList<T>(
                    strQuery,
                    inputParameters);
            }
            catch (Exception e)
            {
                m_lc.Write("Error executing select into data table.");
                m_lc.Write(e);
                m_lc.Write("Sql: " + strQuery);
                if (inputParameters != null)
                {
                    m_lc.Write("Parameters");

                    foreach (DbInputParameter dbInputParameter in inputParameters)
                    {
                        m_lc.Write(dbInputParameter.Name + "=" +
                                   dbInputParameter.Value);
                    }
                }
                throw;
            }
            finally
            {
                //Release connection to pool
                if (conn != null)
                {
                    Release(conn);
                }
            }
        }


        protected virtual void Dispose(bool disposing)
        {
            if (!m_disposed)
            {
                if (m_dbConnection != null)
                {
                    Release(m_dbConnection);
                }
                m_disposed = true;
            }
        }

        /// <summary>
        ///   Destructor
        /// </summary>
        ~AbstractDataService()
        {
            Dispose(false);
        }
    }
}
