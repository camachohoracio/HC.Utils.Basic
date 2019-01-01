#region

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using HC.Utils.Basic.Data.Db.DbConnections;
using HC.Utils.Basic.Data.Db.Parameters;

#endregion

namespace HC.Utils.Basic.Data.Db
{
    public class DbDataReaderWrapper : DbDataReader, IDisposable
    {
        #region Members

        private readonly DbConnectionWrapper m_connection;
        private readonly DbDataReader m_dbDataReader;
        private bool m_blnIsDisposed;

        #endregion

        #region Constructors

        public DbDataReaderWrapper(
            string strQuery,
            string connectionString,
            string strServerName,
            string strDbName,
            DbProviderType dbProviderType,
            List<IDbParameter> inputParameters)
        {
            //
            // get connection from pool
            //
            m_connection = DbConnectionService.Reserve(
                strServerName,
                strDbName,
                connectionString,
                dbProviderType);

            var connection = (IDbConnection) m_connection;

            // load data reader
            m_dbDataReader =
                DbCommandFactory.BuildDbCommand(
                    dbProviderType,
                    strQuery,
                    connection as DbConnection,
                    false,
                    inputParameters).ExecuteReader();

            m_blnIsDisposed = false;
        }

        public DbDataReaderWrapper(
            DbConnectionWrapper dbConnectionWrapper,
            DbCommand dbCommand)
        {
            m_connection = dbConnectionWrapper;
            m_dbDataReader = dbCommand.ExecuteReader();
            m_blnIsDisposed = false;
        }

        #endregion

        public override int Depth
        {
            get { return m_dbDataReader.Depth; }
        }

        public override int FieldCount
        {
            get { return m_dbDataReader.FieldCount; }
        }

        public override bool HasRows
        {
            get { return m_dbDataReader.HasRows; }
        }

        public override bool IsClosed
        {
            get { return m_dbDataReader.IsClosed; }
        }

        public override object this[int ordinal]
        {
            get { return m_dbDataReader[ordinal]; }
        }

        public override int RecordsAffected
        {
            get { return m_dbDataReader.RecordsAffected; }
        }

        public override object this[string name]
        {
            get { return m_dbDataReader[name]; }
        }

        public override int VisibleFieldCount
        {
            get { return m_dbDataReader.VisibleFieldCount; }
        }

        public override DataTable GetSchemaTable()
        {
            return m_dbDataReader.GetSchemaTable();
        }

        public override int GetOrdinal(string name)
        {
            return m_dbDataReader.GetOrdinal(name);
        }

        public override Type GetFieldType(int ordinal)
        {
            return m_dbDataReader.GetFieldType(ordinal);
        }

        public override IEnumerator GetEnumerator()
        {
            return m_dbDataReader.GetEnumerator();
        }

        public override bool Read()
        {
            return m_dbDataReader.Read();
        }

        public override bool NextResult()
        {
            return m_dbDataReader.NextResult();
        }

        public override bool IsDBNull(int ordinal)
        {
            return m_dbDataReader.IsDBNull(ordinal);
        }

        public override int GetValues(object[] values)
        {
            return m_dbDataReader.GetValues(
                values);
        }

        public override object GetValue(int ordinal)
        {
            return m_dbDataReader.GetValue(ordinal);
        }

        public override string GetString(int ordinal)
        {
            return m_dbDataReader.GetString(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return m_dbDataReader.GetInt64(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return m_dbDataReader.GetInt32(ordinal);
        }

        public override short GetInt16(int ordinal)
        {
            return m_dbDataReader.GetInt16(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return m_dbDataReader.GetGuid(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return m_dbDataReader.GetFloat(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return m_dbDataReader.GetDouble(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return m_dbDataReader.GetDecimal(ordinal);
        }

        public override bool GetBoolean(int ordinal)
        {
            return m_dbDataReader.GetBoolean(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return m_dbDataReader.GetByte(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return m_dbDataReader.GetBytes(
                ordinal,
                dataOffset,
                buffer,
                bufferOffset,
                length);
        }

        public override char GetChar(int ordinal)
        {
            return m_dbDataReader.GetChar(ordinal);
        }

        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return m_dbDataReader.GetChars(
                ordinal,
                dataOffset,
                buffer,
                bufferOffset,
                length);
        }

        public override string GetDataTypeName(int ordinal)
        {
            return m_dbDataReader.GetDataTypeName(ordinal);
        }

        public override DateTime GetDateTime(int ordinal)
        {
            return m_dbDataReader.GetDateTime(ordinal);
        }

        public override string GetName(int ordinal)
        {
            return m_dbDataReader.GetName(ordinal);
        }

        public override void Close()
        {
            Dispose();
        }

        #region Destructors

        public new void Dispose()
        {
            HC.Core.EventHandlerHelper.RemoveAllEventHandlers(this);
            if (!m_blnIsDisposed)
            {
                //
                // close db reader
                //
                if (m_dbDataReader != null)
                {
                    if (!m_dbDataReader.IsClosed)
                    {
                        m_dbDataReader.Close();
                        m_dbDataReader.Dispose();
                    }
                }
                //
                // release DB connection
                //
                DbConnectionService.Release(
                    m_connection);

                m_blnIsDisposed = true;
            }
        }

        ~DbDataReaderWrapper()
        {
            Dispose();
        }

        #endregion
    }
}
