#region

using System;
using System.Data;
using System.IO;

#endregion

namespace HC.Utils.Basic.Data.Db
{
    public class CsvReader : IDataReader
    {
        #region Members

        private readonly bool m_blnHasHeaders;
        private readonly char m_delimiter;
        private readonly StreamReader m_sr;
        private bool m_blnInitialize;
        private bool m_blnIsClosed;
        private string m_strCurrentLine;
        private string[] m_strTokens;

        #endregion

        #region Constructors

        public CsvReader(
            string strFileName) :
                this(
                strFileName,
                false,
                ',')
        {
        }

        public CsvReader(
            string strFileName,
            bool blnHasHeaders,
            char delimiter) :
                this(
                new StreamReader(strFileName),
                blnHasHeaders,
                delimiter)
        {
        }

        public CsvReader(
            StreamReader sr,
            bool blnHasHeaders,
            char delimiter)
        {
            m_delimiter = delimiter;
            m_sr = sr;
            m_blnHasHeaders = blnHasHeaders;
            //
            // read header
            //
            if (m_blnHasHeaders)
            {
                sr.ReadLine();
            }
            m_blnInitialize = true;
            GetFieldCount();
        }

        #endregion

        #region Private

        private int GetFieldCount()
        {
            ReadTokens();
            return m_strTokens.Length;
        }

        #endregion

        #region IDataReader Members

        public void Close()
        {
            if (!m_blnIsClosed)
            {
                m_sr.Close();
                m_blnIsClosed = true;
            }
        }

        public int Depth
        {
            get { throw new NotImplementedException(); }
        }

        public DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        public bool IsClosed
        {
            get { return m_blnIsClosed; }
        }

        public bool NextResult()
        {
            throw new NotImplementedException();
        }

        public bool Read()
        {
            if (!m_blnInitialize)
            {
                ReadTokens();

                return (m_strCurrentLine) != null;
            }
            else
            {
                m_blnInitialize = false;
                return m_strTokens != null;
            }
        }

        public int RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }

        public void Dispose()
        {
            if (!m_blnIsClosed)
            {
                Close();
            }
        }

        public int FieldCount
        {
            get { return m_strTokens.Length; }
        }

        public bool GetBoolean(int i)
        {
            return Convert.ToBoolean(
                m_strTokens[i]);
        }

        public byte GetByte(int i)
        {
            return Convert.ToByte(
                m_strTokens[i]);
        }

        public long GetBytes(
            int i,
            long fieldOffset,
            byte[] buffer,
            int bufferoffset,
            int length)
        {
            return Convert.ToByte(
                m_strTokens[i]);
        }

        public char GetChar(int i)
        {
            throw new NotImplementedException();
        }

        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        public IDataReader GetData(int i)
        {
            throw new NotImplementedException();
        }

        public string GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        public DateTime GetDateTime(int i)
        {
            return Convert.ToDateTime(
                m_strTokens[i]);
        }

        public decimal GetDecimal(int i)
        {
            return Convert.ToDecimal(
                m_strTokens[i]);
        }

        public double GetDouble(int i)
        {
            return Convert.ToDouble(
                m_strTokens[i]);
        }

        public Type GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        public float GetFloat(int i)
        {
            return Convert.ToInt64(
                m_strTokens[i]);
        }

        public Guid GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        public short GetInt16(int i)
        {
            return Convert.ToInt16(
                m_strTokens[i]);
        }

        public int GetInt32(int i)
        {
            return Convert.ToInt32(
                m_strTokens[i]);
        }

        public long GetInt64(int i)
        {
            return Convert.ToInt64(
                m_strTokens[i]);
        }

        public string GetName(int i)
        {
            throw new NotImplementedException();
        }

        public int GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        public string GetString(int i)
        {
            return m_strTokens[i];
        }

        public object GetValue(int i)
        {
            return m_strTokens[i];
        }

        public int GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        public bool IsDBNull(int i)
        {
            throw new NotImplementedException();
        }

        public object this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        public object this[int i]
        {
            get { throw new NotImplementedException(); }
        }

        #endregion

        private void ReadTokens()
        {
            m_strCurrentLine = m_sr.ReadLine();
            if (m_strCurrentLine != null)
            {
                //
                // generate tokens
                //
                m_strTokens = m_strCurrentLine.Split(m_delimiter);
            }
            else
            {
                m_strTokens = null;
            }
        }
    }
}
