#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using HC.Core;
using HC.Core.Cache.SqLite;
using HC.Core.DynamicCompilation;
using HC.Core.Exceptions;
using HC.Core.Io;
using HC.Core.Io.Serialization.Types;
using HC.Core.Logging;
using HC.Core.Threading.ProducerConsumerQueues.Support;
using HC.Core.Time;

#endregion

namespace HC.Utils.Basic.Data.Caches.TsCache
{
    public class TsSqLiteCache<T> : TsCsvCache
    {
        #region Members

        private readonly string m_strDbFileName;
        private static readonly ConcurrentDictionary<string, object> m_dirChecked;
        private readonly string m_strTypeName;
        private static readonly Dictionary<string, EnumSqLiteCacheType> m_cacheTypes;
        private readonly EnumSqLiteCacheType m_enumSqLiteCacheType;

        #endregion

        #region Constructors

        static TsSqLiteCache()
        {
            m_cacheTypes = new Dictionary<string, EnumSqLiteCacheType>();
            var cacheList = Basic.Config.GetCacheTypeList();
            foreach (string strItem in cacheList)
            {
                string[] tokens = strItem.Split(',');
                m_cacheTypes[tokens[0]] = (EnumSqLiteCacheType)Enum.Parse(typeof(EnumSqLiteCacheType), tokens[1]);
            }
            m_dirChecked = new ConcurrentDictionary<string, object>();
        }

        public TsSqLiteCache(
            string strFileName,
            EnumSqLiteCacheType cacheType) : this(
            new FileInfo(strFileName).DirectoryName,
            typeof(T),
            false,
            null,
            cacheType,
            typeof(T).Name,
            new FileInfo(strFileName).Name)
        {
        }

        public TsSqLiteCache(
            string strPath,
            string strProviderType,
            EnumSqLiteCacheType cacheType)
            : this(
                strPath,
                null,
                false,
                null,
                strProviderType,
                cacheType)
        {
        }

        public TsSqLiteCache(
            string strPath,
            Type tsDataProviderType,
            bool blnZipFile,
            string[] strTitles)
            : this(
                    strPath,
                    tsDataProviderType,
                    blnZipFile,
                    strTitles,
                    tsDataProviderType.Name) { }

        public TsSqLiteCache(
            string strPath,
            Type tsDataProviderType,
            bool blnZipFile,
            string[] strTitles,
            string strProviderType)
            : this(
                strPath,
                tsDataProviderType,
                blnZipFile,
                strTitles,
                strProviderType,
                GetCacheType(strProviderType))
        {
        }

        public TsSqLiteCache(
            string strPath,
            Type tsDataProviderType,
            bool blnZipFile,
            string[] strTitles,
            string strProviderType,
            EnumSqLiteCacheType enumSqLiteCacheType) :
            this(
            FileHelper.CleanFileName(Path.Combine(
                        Path.Combine(
                            strPath,
                            enumSqLiteCacheType.ToString()),
                        strProviderType)),
            tsDataProviderType,
            blnZipFile,
            strTitles,
            enumSqLiteCacheType,
            typeof(T).Name,
            typeof(T).Name + ".db")
        {
            
        }

        public TsSqLiteCache(
            string strPath,
            Type tsDataProviderType,
            bool blnZipFile,
            string[] strTitles,
            EnumSqLiteCacheType enumSqLiteCacheType,
            string strTypeName,
            string strDbFileName)
            : base(
                strPath,
                tsDataProviderType,
                blnZipFile,
                strTitles)
        {
            try
            {
                m_strTypeName = strTypeName;
                m_strDbFileName = strDbFileName;
                m_strPath = strPath;
                m_enumSqLiteCacheType = enumSqLiteCacheType;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
        }

        public void SetPath(string strPath)
        {
            m_strPath = strPath;
        }

        private static EnumSqLiteCacheType GetCacheType(string strProviderType)
        {
            EnumSqLiteCacheType enumSqLiteCacheType;
            if (!m_cacheTypes.TryGetValue(strProviderType, out enumSqLiteCacheType))
            {
                enumSqLiteCacheType = EnumSqLiteCacheType.FullSchema;
            }
            return enumSqLiteCacheType;
        }

        #endregion

        #region Properties

        public override int Count
        {
            get
            {
                ISqLiteCache<T> dbCache = GetSqLiteCache(string.Empty);
                return dbCache.Count;
            }
        }

        public override int GetCount(DateTime dateTime)
        {
            {
                ISqLiteCache<T> dbCache = GetSqLiteCache(dateTime);
                return dbCache.Count;
            }
        }

        #endregion

        #region Public

        public override Dictionary<string, List<ITsEvent>> GetAllMap(DateTime dateTime)
        {
            try
            {
                var dbCache = GetCache(dateTime);
                //var keys = dbCache.LoadAllKeys();
                //dbCache.LoadDataFromKeys(keys);
                //Console.WriteLine(keys);
                Dictionary<string, List<T>> events = dbCache.LoadAllDataMap();
                var tsEvents = new Dictionary<string, List<ITsEvent>>();
                foreach (KeyValuePair<string, List<T>> keyValuePair in events)
                {
                    SetClassNames(keyValuePair.Value);
                    tsEvents[keyValuePair.Key] = (from n in keyValuePair.Value select (ITsEvent) n).ToList();
                }
                return tsEvents;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return new Dictionary<string, List<ITsEvent>>();
        }

        public override string GetFileName(DateTime dateTime)
        {
            try
            {
                string strDateDir = DateHelper.ToDateString(
                    dateTime);
                string strPath = Path.Combine(
                    m_strPath,
                    strDateDir);
                string strFileName = FileHelper.CleanFileName(Path.Combine(strPath,
                                                  m_strDbFileName));

                return strFileName;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return string.Empty;
        }

        public override List<ITsEvent> GetAll(string strWhere)
        {
            try
            {
                ISqLiteCache<T> dbCache = GetDbCache();
                List<T> events = dbCache.LoadDataFromWhere(strWhere);
                SetClassNames(events);
                return (from n in events select (ITsEvent) n).ToList();
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return new List<ITsEvent>();
        }

        public override Dictionary<string, List<ITsEvent>> GetAllMap(string strWhere)
        {
            try
            {
                ISqLiteCache<T> dbCache = GetDbCache();
                Dictionary<string, List<T>> events = dbCache.LoadDataMapFromWhere(strWhere);
                var resultsMap = new Dictionary<string, List<ITsEvent>>();
                foreach (var kvp in events)
                {
                    List<T> source = kvp.Value;
                    SetClassNames(source);
                    resultsMap[kvp.Key] = kvp.Value.Cast<ITsEvent>().ToList();
                }
                return resultsMap;
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
            return new Dictionary<string, List<ITsEvent>>();
        }

        public ISqLiteCache<T> GetDbCache()
        {
            try
            {
                string strDateDir;
                if (!string.IsNullOrEmpty(DefaultSubPath))
                {
                    strDateDir = DefaultSubPath;
                }
                else
                {
                    strDateDir = DateHelper.ToDateString(new DateTime());
                }

                string strPath = FileHelper.CleanFileName(Path.Combine(
                    m_strPath,
                    strDateDir));
                ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                    Path.Combine(strPath,
                                 m_strDbFileName),
                    m_strTypeName,
                    m_enumSqLiteCacheType,
                    CompressItems);
                return dbCache;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }

        public override bool ContainsKey(
            string strKey)
        {
            ISqLiteCache<T> dbCache = GetSqLiteCache(strKey);
            return dbCache.ContainsKey(strKey);
        }

        public override List<string> ContainsKeys(
            List<string> strKeys)
        {
            if(strKeys == null || strKeys.Count == 0)
            {
                return new List<string>();
            }

            ISqLiteCache<T> dbCache = GetSqLiteCache(strKeys[0]);
            return dbCache.ContainsKeys(strKeys);
        }

        public ISqLiteCache<T> GetSqLiteCache(string strKey)
        {
            string strDateDir = GetDateFromKey(strKey);
            return SqLiteCache(strDateDir);
        }

        public ISqLiteCache<T> GetSqLiteCache(DateTime dateTime)
        {
            string strDateDir = DateHelper.ToDateString(dateTime);
            return SqLiteCache(strDateDir);
        }

        private ISqLiteCache<T> SqLiteCache(string strDateDir)
        {
            try
            {
                string strPath = FileHelper.CleanFileName(Path.Combine(
                    m_strPath,
                    strDateDir));
                ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                    Path.Combine(strPath,
                                 m_strDbFileName),
                    m_strTypeName,
                    m_enumSqLiteCacheType,
                    CompressItems);
                return dbCache;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }

        public void CreateDefaultIndex(string strKey)
        {
            try
            {
                var strDateDir = GetDateFromKey(strKey);
                string strPath = FileHelper.CleanFileName(Path.Combine(
                    m_strPath,
                    strDateDir));
                ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                    Path.Combine(strPath,
                                 m_strDbFileName),
                    m_strTypeName,
                    m_enumSqLiteCacheType,
                    CompressItems);
                dbCache.UseCompression = CompressItems;
                dbCache.CreateIndex(SqliteConstants.KEY_COL_NAME);
                dbCache.CreateIndex("Time");
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
        }

        public void DropDefaultIndex(string strKey)
        {
            try
            {
                var strDateDir = GetDateFromKey(strKey);
                var strPath = Path.Combine(
                    m_strPath,
                    strDateDir);
                ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                    Path.Combine(strPath,
                                 m_strDbFileName),
                    m_strTypeName,
                    m_enumSqLiteCacheType,
                    CompressItems);
                dbCache.DropDefaultIndex();
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
        }

        public override void Delete(List<string> strKeys)
        {
            try
            {

                var pathMap = new Dictionary<string,
                    HashSet<string>>();
                foreach (string strKey in strKeys)
                {
                    string strDateDir = GetDateFromKey(strKey);
                    string strPath =
                        FileHelper.CleanFileName(
                            Path.Combine(
                                m_strPath,
                                strDateDir));

                    object objCheck;
                    if (!m_dirChecked.TryGetValue(strPath, out objCheck))
                    {
                        if (!DirectoryHelper.Exists(strPath))
                        {
                            DirectoryHelper.CreateDirectory(strPath);
                        }
                        m_dirChecked[strPath] = null;
                    }
                    HashSet<string> currEventMap;
                    if (!pathMap.TryGetValue(strPath, out currEventMap))
                    {
                        currEventMap = new HashSet<string>();
                        pathMap[strPath] = currEventMap;
                    }
                    currEventMap.Add(strKey);
                }

                foreach (KeyValuePair<string, HashSet<string>> keyValuePair in pathMap)
                {
                    ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                        Path.Combine(keyValuePair.Key,
                                     m_strDbFileName),
                        m_strTypeName,
                        m_enumSqLiteCacheType,
                        CompressItems);
                    dbCache.Delete(keyValuePair.Value.ToList());
                    string strMessage =
                        ComplexTypeParser.ToStringType(GetType()) + " deleted [" + strKeys.Count + "] keys";
                    Verboser.WriteLine(strMessage);
                }
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
        }

        public override void Delete(string strKey)
        {
            try
            {
                var strDateDir = GetDateFromKey(strKey);
                var strPath = FileHelper.CleanFileName(Path.Combine(
                    m_strPath,
                    strDateDir));
                ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                    Path.Combine(strPath,
                                 m_strDbFileName),
                    m_strTypeName,
                    m_enumSqLiteCacheType,
                    CompressItems);
                dbCache.Delete(strKey);

                string strMessage = ComplexTypeParser.ToStringType(GetType()) + " deleted key [" + strKey + "]";
                Logger.Log(strMessage);
                Console.WriteLine(strMessage);
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
        }

        public override List<ITsEvent> Get(List<string> keyList)
        {
            //
            // group elements by path
            //
            var pathToKeyMap = new Dictionary<string,
                List<string>>();
            foreach (string strKey in keyList)
            {
                //
                // check directory
                //
                string strPath = GetStrPath(strKey);
                List<string> currKeyList;
                if (!pathToKeyMap.TryGetValue(strPath, out currKeyList))
                {
                    currKeyList = new List<string>();
                    pathToKeyMap[strPath] = currKeyList;
                }
                currKeyList.Add(strKey);
            }

            var resulst = new List<T>();
            foreach (KeyValuePair<string, List<string>> keyValuePair in pathToKeyMap)
            {
                ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                    Path.Combine(keyValuePair.Key,
                                 m_strDbFileName),
                    m_strTypeName,
                    m_enumSqLiteCacheType,
                CompressItems);
                var list = dbCache.LoadDataFromKeys(keyValuePair.Value);

                if (list.Count > 0)
                {
                    if (typeof(ASelfDescribingClass).IsAssignableFrom(
                        typeof(T)))
                    {
                        foreach (T aSelfDescribingClass in list)
                        {
                            (aSelfDescribingClass as ASelfDescribingClass).SetClassName(typeof(T).Name);
                            resulst.Add(aSelfDescribingClass);
                        }
                    }
                    else
                    {
                        resulst.AddRange(
                            list);
                    }
                }
            }
            return (from n in resulst select (ITsEvent)n).ToList();
        }

        private string GetStrPath(string strKey)
        {
            try
            {
                var strDateDir = GetDateFromKey(strKey);
                var strPath = FileHelper.CleanFileName(Path.Combine(
                    m_strPath,
                    strDateDir));
                object objCheck;
                if (!m_dirChecked.TryGetValue(strPath, out objCheck))
                {
                    if (!DirectoryHelper.Exists(strPath))
                    {
                        DirectoryHelper.CreateDirectory(strPath);
                    }
                    m_dirChecked[strPath] = null;
                }
                return strPath;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return string.Empty;
        }

        public override void Shrink(DateTime dateTime)
        {
            string strDateDir;
            if (!string.IsNullOrEmpty(DefaultSubPath))
            {
                strDateDir = DefaultSubPath;
            }
            else
            {
                strDateDir = DateHelper.ToDateString(dateTime);
            }

            string strPath = FileHelper.CleanFileName(Path.Combine(
                m_strPath,
                strDateDir));
            ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                Path.Combine(strPath,
                             m_strDbFileName),
                m_strTypeName,
                m_enumSqLiteCacheType,
                CompressItems);
            dbCache.ShrinkDb();
        }

        public override void Clear()
        {
            Clear(new DateTime());
        }

        public override void Clear(DateTime dateTime)
        {
            ISqLiteCache<T> dbCache = GetCache(dateTime);
            dbCache.TrunkateTable(m_strTypeName);
            //var strPath = Path.Combine(
            //    m_strPath,
            //    DateHelper.ToDateString(dateTime));
            //ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
            //    Path.Combine(strPath,
            //                 m_strDbFileName),
            //    m_strTypeName,
            //    m_enumSqLiteCacheType);
            //dbCache.TrunkateTable(m_strTypeName);
        }

        public override List<TaskWrapper> AddToTask(
            Dictionary<string, List<ITsEvent>> mapKeyToEvents)
        {
            try
            {
                //
                // group elements by path
                //
                var pathMap = new Dictionary<string,
                    Dictionary<string, List<T>>>();
                var taskList = new List<TaskWrapper>();
                foreach (string strKey in mapKeyToEvents.Keys)
                {
                    string strDateDir = GetDateFromKey(strKey);
                    string strPath = Path.Combine(
                        m_strPath,
                        strDateDir);
                    strPath = FileHelper.CleanFileName(strPath);

                    object objCheck;
                    if (!m_dirChecked.TryGetValue(strPath, out objCheck))
                    {
                        if (!DirectoryHelper.Exists(strPath))
                        {
                            try
                            {
                                DirectoryHelper.CreateDirectory(strPath);
                            }
                            catch (Exception ex)
                            {
                                Logger.Log(ex);
                                throw new HCException("Failed to create dir[" +
                                                      strPath + "] " +
                                                      ex.Message +
                                                      ex.StackTrace);
                            }
                        }
                        m_dirChecked[strPath] = null;
                    }
                    Dictionary<string, List<T>> currEventMap;
                    if (!pathMap.TryGetValue(strPath, out currEventMap))
                    {
                        currEventMap = new Dictionary<string, List<T>>();
                        pathMap[strPath] = currEventMap;
                    }
                    List<T> currEventList;
                    if (!currEventMap.TryGetValue(strKey, out currEventList))
                    {
                        currEventList = new List<T>();
                        currEventMap[strKey] = currEventList;
                    }
                    currEventList.AddRange(
                        from n in
                            mapKeyToEvents[strKey]
                        select (T) n);
                }

                foreach (KeyValuePair<string, Dictionary<string, List<T>>> keyValuePair in pathMap)
                {
                    ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                        Path.Combine(keyValuePair.Key,
                                     m_strDbFileName),
                        m_strTypeName,
                        m_enumSqLiteCacheType,
                        CompressItems);
                    taskList.AddRange(dbCache.Insert(
                        CloneMap(keyValuePair.Value)));
                    keyValuePair.Value.Clear();
                }
                pathMap.Clear();
                return taskList;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return new List<TaskWrapper>();
        }

        private Dictionary<string, List<T>> CloneMap(Dictionary<string, List<T>> value)
        {
            return value.ToDictionary(t=>t.Key,t=>t.Value);
        }

        public override TaskWrapper AddToTask(
            string strKey,
            ITsEvent tsEvent)
        {
            return AddToTask(strKey,
                new List<ITsEvent>(new[] { tsEvent }));
        }

        public override TaskWrapper AddToTask(
            string strKey,
            List<ITsEvent> events)
        {
            try
            {
                var strDateDir = GetDateFromKey(strKey);
                var strPath = FileHelper.CleanFileName(Path.Combine(
                    m_strPath,
                    strDateDir));
                strPath = FileHelper.CleanFileName(strPath);

                object objCheck;
                if (!m_dirChecked.TryGetValue(strPath, out objCheck))
                {
                    if (!DirectoryHelper.Exists(strPath))
                    {
                        DirectoryHelper.CreateDirectory(strPath);
                    }
                    m_dirChecked[strPath] = null;
                }
                ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                    Path.Combine(strPath,
                                 m_strDbFileName),
                    m_strTypeName,
                    m_enumSqLiteCacheType,
                CompressItems);
                return dbCache.Insert(
                    strKey,
                    (from n in events select (T) n).ToList());
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }

        public override List<ITsEvent> Get(string strKey)
        {
            try
            {
                var strDateDir = GetDateFromKey(strKey);
                var strPath = FileHelper.CleanFileName(Path.Combine(
                    m_strPath,
                    strDateDir));
                ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                    Path.Combine(strPath,
                                 m_strDbFileName),
                    m_strTypeName,
                    m_enumSqLiteCacheType,
                    CompressItems);
                var events = dbCache.LoadDataFromKey(
                    strKey);

                SetClassNames(events);


                return (from n in events select (ITsEvent) n).ToList();
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return new List<ITsEvent>();
        }

        private void SetClassNames(List<T> events)
        {
            if (events.Count > 0)
            {
                if ((events[0] as ASelfDescribingClass) != null)
                {
                    foreach (var selfDescribingClass in events)
                    {
                        (selfDescribingClass as ASelfDescribingClass).SetClassName(
                            selfDescribingClass.GetType().Name);
                    }
                }
            }
        }

        public override int GetRowCount(string strWhere)
        {
            ISqLiteCache<T> dbCache = GetDbCache();
            return dbCache.GetCountFromWhere(strWhere);
        }

        public override int GetRowCount(DateTime dateTime)
        {
            return GetKeysFromDate(dateTime).Count;
        }

        public override List<string> GetKeysFromDate(
            DateTime startDateTime)
        {
            ISqLiteCache<T> dbCache = GetCache(startDateTime);

            var keyList =
                new List<string>();
            keyList.AddRange(dbCache.LoadAllKeys());

            return keyList;
        }

        public override List<ITsEvent> GetAll()
        {
            return GetAll(new DateTime());
        }


        public override List<ITsEvent> GetAll(DateTime dateTime)
        {
            try
            {
                ISqLiteCache<T> dbCache = GetCache(dateTime);
                List<T> events = dbCache.LoadAllData();
                if(events == null ||
                    events.Count == 0)
                {
                    return new List<ITsEvent>();
                }
                SetClassNames(events);
                return (from n in events select (ITsEvent) n).ToList();
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return new List<ITsEvent>();
        }

        public ISqLiteCache<T> GetCache()
        {
            return GetCache(new DateTime());
        }

        public ISqLiteCache<T> GetCache(DateTime dateTime)
        {
            try
            {
                string strDateDir;
                if (!string.IsNullOrEmpty(DefaultSubPath))
                {
                    strDateDir = DefaultSubPath;
                }
                else
                {
                    strDateDir = DateHelper.ToDateString(dateTime);
                }
                var strPath = FileHelper.CleanFileName(Path.Combine(
                    m_strPath,
                    strDateDir));
                ISqLiteCache<T> dbCache = SqLiteCacheFactory.GetSqLiteDb<T>(
                    Path.Combine(strPath,
                                 m_strDbFileName),
                    m_strTypeName,
                    m_enumSqLiteCacheType,
                    CompressItems);
                return dbCache;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }

        #endregion
    }
}

