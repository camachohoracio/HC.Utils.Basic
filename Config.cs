using HC.Core.ConfigClasses;
using System.Collections.Generic;

namespace HC.Utils.Basic
{
    public static class Config
    {
        public static int GetPoolCapacity()
        {
            return HCConfig.GetConstant<int>(
                "PoolCapacity",
                typeof(Config));
        }


        public static string GetSerializedDbPath()
        {
            return HCConfig.GetConstant<string>(
                "BtDbPath",
                typeof(Config));
        }
        public static List<string> GetCacheTypeList()
        {
            return HCConfig.GetConfigList(
                "CacheTypes",
                typeof(Config));
        }
        public static List<string> GetAssemblyList()
        {
            return HCConfig.GetConfigList(
                "AssemblyList",
                typeof(Config));
        }

    }
}
