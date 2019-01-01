#region

using System;
using HC.Analytics.TimeSeries;
using HC.Core.Resources;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    public interface IDataProvider : IResource
    {
        #region Interface Methods

        ITsEvents LoadData(TsDataRequest tsDataRequest);
        Type GetTsEventType();

        #endregion
    }
}
