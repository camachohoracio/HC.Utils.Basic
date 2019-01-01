#region

#region

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using HC.Core.Exceptions;
using HC.Core.Logging;
using HC.Core.Resources;
using HC.Utils.Basic.Data.Db.Parameters;

#endregion

#endregion

namespace HC.Utils.Basic.Data.Db.DbConnections
{
    [Serializable]
    public class DbConnectionWrapper : IResource, IDisposable
    {
        #region Properties

        private static readonly ILoggerService m_lc = Logger.GetLogger();
        public IDbConnection DbConnection { get; set; }
        public IDataRequest DataRequest { get; set; }
        public Object Owner { get; set; }
        public DateTime TimeUsed { get; set; }
        public bool HasChanged { get; set; }

        #endregion

        #region Members

        private DbCommand m_dbCommand;
        private DbProviderType m_dbProviderType;

        #endregion

        #region Constructors

        public DbConnectionWrapper(
            DbProviderType dbProviderType,
            string strConnectionString)
        {
            // initialize class
            Initialize(dbProviderType,
                       strConnectionString);
        }

        #endregion

        #region Public

        public void Close()
        {
            Dispose();
        }

        public int Insert(
            string strQuery,
            bool blnTransaction)
        {
            return Insert(
                strQuery,
                null,
                blnTransaction);
        }

        public int Insert(
            string strQuery)
        {
            return Insert(
                strQuery,
                null,
                false);
        }

        public int Insert(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            return Insert(
                strQuery,
                inputParameters,
                true);
        }

        public void ExecuteNonQuery(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            ExecuteNonQuery(
                strQuery,
                inputParameters,
                true);
        }

        public void ExecuteNonQuery(
            string strQuery)
        {
            ExecuteNonQuery(
                strQuery,
                null);
        }

        public void ExecuteNonQuery(
            string strQuery,
            bool blnTrans)
        {
            ExecuteNonQuery(
                strQuery,
                null,
                blnTrans);
        }

        public void SelectInto(
            string strQuery,
            DataTable dataTable)
        {
            SelectInto(
                strQuery,
                null,
                dataTable);
        }

        public void SelectInto(
            string strQuery,
            List<IDbParameter> inputParameters,
            DataTable dataTable)
        {
            lock (DbConnection)
            {
                if (m_dbCommand != null)
                {
                    throw new HCException("DB Command in use.");
                }

                try
                {
                    m_dbCommand = DbCommandFactory.BuildDbCommand(
                        m_dbProviderType,
                        strQuery,
                        DbConnection as DbConnection,
                        false,
                        inputParameters);

                    if (m_dbCommand != null)
                    {
                        dataTable.Load(m_dbCommand.ExecuteReader());
                    }
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
                    if (m_dbCommand != null)
                    {
                        m_dbCommand.Dispose();
                        m_dbCommand = null;
                    }
                }
            }
        }

        public T SelectValue<T>(
            string strQuery)
        {
            return SelectValue<T>(
                strQuery,
                null);
        }

        public List<T> SelectValueList<T>(
            string strQuery)
        {
            return SelectValueList<T>(
                strQuery,
                null);
        }

        public List<T> SelectValueList<T>(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            lock (DbConnection)
            {
                if (m_dbCommand != null)
                {
                    throw new HCException("DB Command in use.");
                }

                try
                {
                    m_dbCommand = DbCommandFactory.BuildDbCommand(
                        m_dbProviderType,
                        strQuery,
                        DbConnection as DbConnection,
                        inputParameters);

                    var dataTable = new DataTable();

                    if (m_dbCommand != null)
                    {
                        dataTable.Load(m_dbCommand.ExecuteReader());
                    }

                    if (dataTable.Rows.Count == 0)
                    {
                        throw new HCException("Error. The query did not return any rows.");
                    }

                    var returnList = new List<T>(dataTable.Rows.Count + 1);
                    foreach (DataRow dr in dataTable.Rows)
                    {
                        returnList.Add((T) dr[0]);
                    }
                    return returnList;
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
                    if (m_dbCommand != null)
                    {
                        m_dbCommand.Dispose();
                        m_dbCommand = null;
                    }
                }
            }
        }


        public T SelectValue<T>(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            lock (DbConnection)
            {
                if (m_dbCommand != null)
                {
                    throw new HCException("DB Command in use.");
                }

                try
                {
                    m_dbCommand = DbCommandFactory.BuildDbCommand(
                        m_dbProviderType,
                        strQuery,
                        DbConnection as DbConnection,
                        inputParameters);

                    var dataTable = new DataTable();

                    if (m_dbCommand != null)
                    {
                        dataTable.Load(m_dbCommand.ExecuteReader());
                    }

                    if (dataTable.Rows.Count == 0)
                    {
                        throw new HCException("Error. The query did not return any rows.");
                    }

                    if (dataTable.Rows.Count > 1)
                    {
                        throw new HCException("More than 1 row returned.");
                    }
                    if (dataTable.Rows[0].ItemArray.Length != 1)
                    {
                        throw new HCException("More than 1 value returned.");
                    }


                    return (T) dataTable.Rows[0][0];
                }
                catch (Exception e)
                {
                    //m_lc.Write("Error executing select into data table.");
                    //m_lc.Write(e);
                    //m_lc.Write("Sql: " + strQuery);
                    if (inputParameters != null)
                    {
                        //m_lc.Write("Parameters");

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
                    if (m_dbCommand != null)
                    {
                        m_dbCommand.Dispose();
                        m_dbCommand = null;
                    }
                }
            }
        }

        public bool ChangeDatabase(string dbName)
        {
            if (dbName != null && dbName != string.Empty)
            {
                try
                {
                    if (DbConnection.State != ConnectionState.Open)
                    {
                        DbConnection.Open();
                    }
                    DbConnection.ChangeDatabase(dbName);
                }
                catch (Exception e)
                {
                    m_lc.Write("Error in changing database name.");
                    m_lc.Write("Database Name : " + dbName);
                    m_lc.Write(e);
                    throw;
                    //return false;
                }
                return true;
            }
            else
            {
                m_lc.Write("Database name is Empty");
                return false;
            }
        }

        public int Insert(
            string strQuery,
            List<IDbParameter> inputParameters,
            bool blnTrans)
        {
            return ExecuteNonQuery(
                strQuery,
                inputParameters,
                blnTrans);
        }

        public int Update(
            string strQuery,
            bool blnTrans)
        {
            return Update(strQuery,
                          null,
                          blnTrans);
        }

        public int Update(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            return Update(strQuery,
                          inputParameters,
                          true);
        }

        public int Update(
            string strQuery)
        {
            return Update(strQuery,
                          null,
                          true);
        }

        public int Update(
            string strQuery,
            List<IDbParameter> inputParameters,
            bool blnTrans)
        {
            return ExecuteNonQuery(
                strQuery,
                inputParameters,
                blnTrans);
        }


        public int Delete(
            string strQuery,
            bool blnTrans)
        {
            return Delete(strQuery,
                          null,
                          blnTrans);
        }

        public int Delete(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            return Delete(strQuery,
                          inputParameters,
                          true);
        }

        public int Delete(
            string strQuery)
        {
            return Delete(strQuery,
                          null,
                          true);
        }

        public int Delete(
            string strQuery,
            List<IDbParameter> inputParameters,
            bool blnTrans)
        {
            return ExecuteNonQuery(
                strQuery,
                inputParameters,
                blnTrans);
        }

        public int ExecuteNonQuery(
            string strQuery,
            List<IDbParameter> inputParameters,
            bool blnTrans)
        {
            var intNewRowsCount = -1;
            lock (DbConnection)
            {
                if (m_dbCommand != null)
                {
                    throw new HCException("DB Command in use.");
                }

                var blnCommited = false;
                DbTransaction transaction = null;
                try
                {
                    m_dbCommand = DbCommandFactory.BuildDbCommand(
                        m_dbProviderType,
                        strQuery,
                        DbConnection as DbConnection,
                        blnTrans,
                        inputParameters);
                    intNewRowsCount = m_dbCommand.ExecuteNonQuery();

                    if (blnTrans)
                    {
                        transaction = m_dbCommand.Transaction;
                        transaction.Commit();
                        blnCommited = true;
                    }
                }
                catch (Exception e)
                {
                    m_lc.Write("Error executing insert.");
                    m_lc.Write(e);
                    m_lc.Write("Sql: " + strQuery);
                    if (inputParameters != null)
                    {
                        m_lc.Write("Parameters");

                        foreach (DbInputParameter dbInputParameter in inputParameters)
                        {
                            IEnumerator enumerator = inputParameters.GetEnumerator();
                            m_lc.Write(dbInputParameter.Name + "=" +
                                       dbInputParameter.Value);
                        }
                    }
                    m_lc.Write("Is a transaction? " + blnTrans);
                    throw;
                }
                finally
                {
                    if (blnTrans && !blnCommited && transaction != null)
                    {
                        transaction.Rollback();
                    }
                    if (m_dbCommand != null)
                    {
                        m_dbCommand.Dispose();
                        m_dbCommand = null;
                    }
                }
                return intNewRowsCount;
            }
        }


        public DbDataReaderWrapper ExecuteReader(
            string strQuery)
        {
            return ExecuteReader(
                strQuery,
                null);
        }

        public DbDataReaderWrapper ExecuteReader(
            string strQuery,
            List<IDbParameter> inputParameters)
        {
            lock (DbConnection)
            {
                if (m_dbCommand != null)
                {
                    throw new HCException("DB Command in use.");
                }

                try
                {
                    m_dbCommand = DbCommandFactory.BuildDbCommand(
                        m_dbProviderType,
                        strQuery,
                        DbConnection as DbConnection,
                        false,
                        inputParameters);

                    return new DbDataReaderWrapper(
                        this,
                        m_dbCommand);
                }
                catch (Exception e)
                {
                    m_lc.Write("Error executing data reader.");
                    m_lc.Write(e);
                    m_lc.Write("Sql: " + strQuery);
                    if (inputParameters != null)
                    {
                        m_lc.Write("Parameters");

                        foreach (DbInputParameter dbInputParameter in inputParameters)
                        {
                            IEnumerator enumerator = inputParameters.GetEnumerator();
                            m_lc.Write(dbInputParameter.Name + "=" +
                                       dbInputParameter.Value);
                        }
                    }
                    throw;
                }
                finally
                {
                    if (m_dbCommand != null)
                    {
                        m_dbCommand.Dispose();
                        m_dbCommand = null;
                    }
                }
            }
        }

        #endregion

        #region Initializer

        private void Initialize(
            DbProviderType dbProviderType,
            string strConnectionString)
        {
            if (!string.IsNullOrEmpty(strConnectionString))
            {
                // create a new connection
                DbConnection =
                    DbConnectionFactory.BuildConnection(
                        dbProviderType,
                        strConnectionString);
            }

            // set db provider type
            m_dbProviderType = dbProviderType;
        }

        #endregion

        #region Dispose

        public void Dispose()
        {
            HC.Core.EventHandlerHelper.RemoveAllEventHandlers(this);
            if (DbConnection != null)
            {
                DbConnection.Dispose();
            }
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
