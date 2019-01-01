#region

using System;
using System.Text;
using HC.Core.Io.Serialization.Interfaces;
using HC.Core.Resources;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    [Serializable]
    public class TsDataProviderRequest : ASerializable, IDataRequest
    {
        #region Members

        private string m_strRequestDescr;

        #endregion

        #region Properties

        /// <summary>
        /// Leave setter public. Used for serialization
        /// </summary>
        public string DataProviderType { get; set; }
        
        /// <summary>
        /// Leave setter public. Used for serialization
        /// </summary>
        public long BarSize { get; set; }

        /// <summary>
        /// Leave setter public. Used for serialization
        /// </summary>
        public long BarOffset { get; set; }

        /// <summary>
        /// Leave setter public. Used for serialization
        /// </summary>
        public bool DoConsolidate { get; set; }

        /// <summary>
        /// Leave setter public. Used for serialization
        /// </summary>
        public bool IsContinuous { get; set; }

        public string Name
        {
            get { return m_strRequestDescr; }
        }

        #endregion

        #region Constructors

        /// <summary>
        /// Leave this constructor. It is used for serialization
        /// </summary>
        public TsDataProviderRequest()
        {
        }

        public TsDataProviderRequest(
            string enumDataProviderType,
            long lngBarSize,
            long lngBarOffset,
            bool blnDoConsolidate,
            bool blnIsContinuous)
        {
            DataProviderType = enumDataProviderType;
            BarSize = lngBarSize;
            BarOffset = lngBarOffset;
            DoConsolidate = blnDoConsolidate;
            IsContinuous = blnIsContinuous;
            SetResourceName();
        }

        #endregion

        #region Private

        private void SetResourceName()
        {
            var sb =
                new StringBuilder();

            sb.Append(BarSize)
                .Append("%")
                .Append(DoConsolidate)
                .Append("%")
                .Append(IsContinuous)
                .Append("%")
                .Append(BarOffset)
                .Append("%")
                .Append(DataProviderType)
                .Append("%");
                //.Append(TimeSeriesDataType);
            m_strRequestDescr = sb.ToString();
        }

        #endregion

        #region Public

        public bool Equals(IDataRequest other)
        {
            return Name.Equals(other.Name);
        }

        public int CompareTo(IDataRequest other)
        {
            return Name.CompareTo(other.Name);
        }

        public int Compare(IDataRequest x, IDataRequest y)
        {
            return x.Name.CompareTo(y.Name);
        }

        public int Compare(object x, object y)
        {
            return Compare((TsDataProviderRequest) x,
                           (TsDataProviderRequest) y);
        }

        public bool Equals(IDataRequest x, IDataRequest y)
        {
            return x.Name.Equals(y.Name);
        }

        public int GetHashCode(IDataRequest obj)
        {
            return obj.Name.GetHashCode();
        }

        #endregion

        public void Dispose()
        {
            m_strRequestDescr = null;
            DataProviderType = null;
            m_strRequestDescr = null;
        }
    }
}
