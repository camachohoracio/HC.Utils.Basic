using System.Collections.Generic;

namespace HC.Utils.Basic.Data.DataProviders
{
    public static class DataProviderConstants
    {
        public static Dictionary<string, int> m_mapDataProviderToQueueSize =
            new Dictionary<string, int>();

        static DataProviderConstants()
        {
            m_mapDataProviderToQueueSize[typeof(GenericTsDataProvider).Name] = 50;
        }
    }
}
