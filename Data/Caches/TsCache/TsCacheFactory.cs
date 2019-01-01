#region

using System;
using System.Collections.Generic;
using HC.Analytics.TimeSeries;
using HC.Core.Cache.SqLite;
using HC.Core.Logging;
using HC.Core.Reflection;
using HC.Utils.Basic.Data.DataProviders;

#endregion

namespace HC.Utils.Basic.Data.Caches.TsCache
{
    public static class TsCacheFactory
    {
        #region Properties

        public static string DefaultDataPath { get; private set; }

        #endregion

        #region Members

        private static readonly Dictionary<string, string> m_dataProviderToPathMap;

        #endregion

        #region Constructors

        static TsCacheFactory()
        {
            DefaultDataPath = Core.Config.GetDefaultCacheDataPath();
            m_dataProviderToPathMap = Core.Config.GetDataProviderToPathMap();
        }

        #endregion

        #region Public

        public static ITsCache BuildSerializerCache(TsDataRequest tsDataRequest)
        {
            Type providerType = TsDataProviderHelper.GetDataProviderType(
                    tsDataRequest.DataProviderType);
            ITsCache cache = BuildSerializerCache(providerType);
            return cache;
        }

        public static ITsCache BuildSerializerCache<T>(
            EnumSqLiteCacheType enumSqLiteCacheType)
        {
            return BuildSerializerCache<T>(typeof(T).Name,
                                        enumSqLiteCacheType);
        }

        public static ITsCache BuildSerializerCache<T>()
        {
            return BuildSerializerCache<T>(typeof (T).Name);
        }

        public static ITsCache BuildSerializerCache<T>(
            string strCacheName)
        {
            return BuildSerializerCache<T>(strCacheName,
                                        EnumSqLiteCacheType.BLob);
        }

        public static ITsCache BuildSerializerCache<T>(
            string strCacheName,
            EnumSqLiteCacheType cacheType)
        {
            var dataCache = new TsSqLiteCache<T>(
                DefaultDataPath,
                strCacheName,
                cacheType)
            {
                DefaultSubPath = typeof(T).Name
            };
            return dataCache;
        }


        public static ITsCache BuildSerializerCache(Type providerType)
        {
            return BuildSerializerCache(providerType,
                                    EnumSqLiteCacheType.None);
        }

        public static ITsCache BuildSerializerCache(Type providerType,
            EnumSqLiteCacheType enumSqLiteCacheType)
        {
            try
            {
                string strPath = GetDataPath(providerType);
                return BuildSerializerCache(providerType,
                                            enumSqLiteCacheType,
                                            strPath);
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }

        public static ITsCache BuildSerializerCache(
            Type providerType,
            EnumSqLiteCacheType enumSqLiteCacheType,
            string strPath)
        {
            try
            {
                IReflector dataProviderReflector = ReflectorCache.GetReflector(
                    providerType);
                Type eventType = ((IDataProvider) dataProviderReflector.CreateInstance()).GetTsEventType();
                Type genericCacheType = typeof (TsSqLiteCache<>);
                Type specificBinderType = genericCacheType.MakeGenericType(eventType);

                if (enumSqLiteCacheType == EnumSqLiteCacheType.None)
                {
                    return (TsCsvCache) Activator.CreateInstance(specificBinderType,
                                                                 new object[]
                                                                     {
                                                                         strPath,
                                                                         providerType,
                                                                         false,
                                                                         null
                                                                     });
                }
                return (TsCsvCache) Activator.CreateInstance(specificBinderType,
                                                             new object[]
                                                                 {
                                                                     strPath,
                                                                     providerType,
                                                                     false,
                                                                     null,
                                                                     providerType.Name,
                                                                     enumSqLiteCacheType
                                                                 });
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }


        public static string GetDataPath(Type providerType)
        {
            string strPath;
            if (!m_dataProviderToPathMap.TryGetValue(providerType.Name, out strPath))
            {
                strPath = DefaultDataPath;
            }
            return strPath;
        }

        #endregion
    }
}
