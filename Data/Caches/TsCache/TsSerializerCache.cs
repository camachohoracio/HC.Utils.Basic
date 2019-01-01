#region

using System.Collections.Generic;
using HC.Core.Cache;
using HC.Core.DynamicCompilation;

#endregion

namespace HC.Utils.Basic.Data.Caches.TsCache
{
    public static class TsSerializerCache
    {
        #region Public

        public static CacheDictionary<string, List<ITsEvent>> BuildSerializedDictBarData()
        {
            return BuildDb("BarData");
        }

        public static CacheDictionary<string, List<ITsEvent>> BuildSerializedDictFxBarData()
        {
            return BuildDb("FxBarData");
        }

        public static CacheDictionary<string, List<ITsEvent>> BuildSerializedDictConsolidatedData()
        {
            return BuildDb("ConsolidatedData");
        }

        public static CacheDictionary<string, List<ITsEvent>> BuildSerializedDictClientData()
        {
            return BuildDb("ClientData");
        }

        public static CacheDictionary<string, List<ITsEvent>> BuildSerializedDictTickData()
        {
            return BuildDb("TickData");
        }

        #endregion

        #region Private

        private static CacheDictionary<string, List<ITsEvent>> BuildDb(
            string strDbName)
        {
            var localDb =
                new CacheDictionary<string, List<ITsEvent>>(
                    strDbName,
                    Config.GetSerializedDbPath(),
                    true);
            return localDb;
        }

        #endregion
    }
}
