#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using HC.Analytics.TimeSeries;
using HC.Core.DynamicCompilation;
using HC.Core.Exceptions;
using HC.Core.Helpers;
using HC.Core.Io.KnownObjects;
using HC.Core.Logging;
using HC.Core.Reflection;
using HC.Core.Threading.Buffer;
using HC.Core.Time;
using HC.Utils.Basic.Data.Caches.TsCache;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    public static class TsDataProviderHelper
    {
        #region Constants

        public const string DUMMY_SYMBOL = "OqDummySymbol";

        #endregion

        #region Members

        private static ConcurrentDictionary<string, Type> m_dataProviderTypes;

        #endregion

        #region Constructors

        static TsDataProviderHelper()
        {
            LoadDataProviderTypes();
        }

        public static string GetDateFromKey(string strKey)
        {
            var regex = new Regex(@"\d{4}_\d{1,2}_\d{1,2}");

            var m = regex.Match(strKey);
            if (m.Success)
            {
                var strDate = m.Captures[0].ToString();
                return strDate;
            }
            return "";
        }

        public static void LoadDataProviderTypes()
        {
            m_dataProviderTypes = new ConcurrentDictionary<string, Type>();
            List<Type> dataProviderTypes = (from n in typeof (IDataProvider).Assembly.GetTypes()
                                            where typeof (IDataProvider).IsAssignableFrom(n) &&
                                                  !n.IsAbstract &&
                                                  !n.IsInterface
                                            select n).ToList();
            foreach (Type dataProviderType in dataProviderTypes)
            {
                m_dataProviderTypes[dataProviderType.Name] = dataProviderType;
            }
            Logger.Log("Loaded " + m_dataProviderTypes.Count + " data providers: " +
                String.Join(",", m_dataProviderTypes.Keys));
        }

        #endregion

        private readonly static object m_lockObject = new object();
        private static bool m_blnIsProviderLoaded;

        public static List<ITsEvent> GetEvsFromBuffer(
            EfficientMemoryBuffer<string, List<ITsEvent>> tsBuffer,
            string strDataProviderName,
            bool blnUseService)
        {
            string strKey = DateTime.Today.ToString();
            List<ITsEvent> tsEvents;
            if (!tsBuffer.TryGetValue(strKey, out tsEvents))
            {
                tsEvents = SearchForItemsInCache(strDataProviderName, blnUseService);
                tsBuffer.Add(strKey, tsEvents);
            }
            return tsEvents;
        }

        private static List<ITsEvent> SearchForItemsInCache(
            string strDataProviderName,
            bool blnUseService)
        {
            try
            {
                if (blnUseService)
                {
                    return (List<ITsEvent>)GenericTsDataProvider.RunMethodDistributedViaService(
                        typeof(TsDataProviderHelper),
                        "SearchForItemsInCacheLocal",
                        new List<object>(new object[] { strDataProviderName}));
                }
                return SearchForItemsInCacheLocal(strDataProviderName);
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
            return new List<ITsEvent>();
        }

        public static List<ITsEvent> SearchForItemsInCacheLocal(
            string strDataProviderName)
        {
            try
            {
                DateTime currDate = DateTime.Today;
                for (int i = 0; i < 20; i++)
                {
                    try
                    {
                        var tsDataRequest =
                            new TsDataRequest
                            {
                                StartTime = currDate,
                                EndTime = DateHelper.GetEndOfDay(currDate),
                                DataProviderType = strDataProviderName
                            };
                        ITsCache cache = TsCacheFactory.BuildSerializerCache(
                            tsDataRequest);
                        if (cache.ContainsKey(tsDataRequest.Name))
                        {
                            var tsEvs = cache.Get(tsDataRequest.Name);
                            if (tsEvs.Count > 1)
                            {
                                return tsEvs;
                            }
                        }
                        currDate = currDate.AddDays(-1);
                    }
                    catch (Exception ex)
                    {
                        Logger.Log(ex);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
            return new List<ITsEvent>();
        }

        public static void LoadDataProvidersTypes()
        {
            if (m_blnIsProviderLoaded)
            {
                return;
            }
            lock (m_lockObject)
            {
                if (m_blnIsProviderLoaded)
                {
                    return;
                }
                GenericTsDataProvider.Initialize();
                var dataProviderTypes = new List<Type>();
                for (int i = 0; i < AssemblyCache.LoadedAssemblies.Count; i++)
                {
                    Assembly currAssembly = AssemblyCache.LoadedAssemblies[i];
                    List<Type> currDataProviderTypes = (from n in currAssembly.GetTypes()
                                                    where typeof(IDataProvider).IsAssignableFrom(n)
                                                    select n).ToList();
                    dataProviderTypes.AddRange(currDataProviderTypes);
                }
                LoadDataProviders(dataProviderTypes);
                m_blnIsProviderLoaded = true;
            }
        }


        public static bool CheckDefaultSymbol(
            TsDataRequest tsDataRequest)
        {
            ITsCache cache = TsCacheFactory.BuildSerializerCache(
                tsDataRequest);
            return CheckDefaultSymbol(tsDataRequest, cache);
        }

        public static bool CheckDefaultSymbol(TsDataRequest tsDataRequest, ITsCache cache)
        {
            tsDataRequest = (TsDataRequest) tsDataRequest.Clone();
            tsDataRequest.Symbols = DUMMY_SYMBOL;
            return cache.ContainsKey(tsDataRequest.Name);
        }

        public static void AddDefaultSymbol(
            TsDataRequest tsDataRequest)
        {
            ITsCache cache = TsCacheFactory.BuildSerializerCache(
                tsDataRequest);
            AddDefaultSymbol(tsDataRequest, cache);
        }

        public static void AddDefaultSymbol(
            TsDataRequest tsDataRequest, 
            ITsCache cache)
        {
            tsDataRequest = (TsDataRequest) tsDataRequest.Clone();
            tsDataRequest.Symbols = DUMMY_SYMBOL;

            var providerType = GetDataProviderType(
                tsDataRequest.DataProviderType);

            var dataProviderReflector = ReflectorCache.GetReflector(
                providerType);
            IDataProvider dataProvider = (ATsDataProvider) dataProviderReflector.CreateInstance();
            var tsEvent = (ITsEvent) ReflectorCache.GetReflector(
                dataProvider.GetTsEventType()).CreateInstance();
            cache.Add(tsDataRequest.Name, tsEvent);
        }

        public static void LoadDataProviders(List<Type> providers)
        {
            try
            {
                foreach (Type dataProviderType in providers)
                {
                    try
                    {
                        m_dataProviderTypes[dataProviderType.Name] = dataProviderType;
                    }
                    catch (Exception ex)
                    {
                        Logger.Log(ex);
                    }
                }
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
        }

        public static void RemoveFromCache(
            TsDataRequest tsDataRequest,
            string[] symbols,
            ITsCache dataCache)
        {
            if (symbols != null && symbols.Length > 0)
            {
                foreach (string strSymbol in symbols)
                {
                    var currTsDataRequest = (TsDataRequest) tsDataRequest.Clone();
                    currTsDataRequest.Symbols = strSymbol;
                    dataCache.Delete(currTsDataRequest.Name);
                }
            }
        }

        public static ITsEvents GetDataFromCache(
            TsDataRequest tsDataRequest,
            string[] symbols,
            ITsCache dataCache,
            out List<string> missingSymbols)
        {
            missingSymbols = null;
            try
            {
                missingSymbols = new List<string>();
                var collectedEvents = new List<ITsEvent>();
                if (symbols != null && symbols.Length > 0)
                {
                    foreach (string strSymbol in symbols)
                    {
                        var currTsDataRequest = (TsDataRequest) tsDataRequest.Clone();
                        currTsDataRequest.Symbols = strSymbol;
                        if (dataCache.ContainsKey(currTsDataRequest.Name))
                        {
                            collectedEvents.AddRange(dataCache.Get(currTsDataRequest.Name));
                        }
                        else
                        {
                            missingSymbols.Add(strSymbol);
                        }
                    }
                    return new TsEvents
                               {
                                   TsEventsList = collectedEvents,
                               };
                }
                //
                // get all events
                //
                collectedEvents.AddRange(dataCache.GetAll(tsDataRequest.StartTime));
                if (collectedEvents.Count > 0)
                {
                    //
                    // return collected events
                    //
                    return new TsEvents
                               {
                                   TsEventsList = collectedEvents,
                               };
                }
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }

        public static Type GetDataProviderType(string strType)
        {
            return m_dataProviderTypes[strType];
        }

        public static List<string> GetDataProvidersDescr()
        {
            return m_dataProviderTypes.Keys.ToList();
        }

        public static bool IsFxRequest(TsDataRequest tsDataRequest)
        {
            if (tsDataRequest.SymbolArr == null)
            {
                return false;
            }

            var blnIsFxRequest = false;
            foreach (string strSymbol in tsDataRequest.SymbolArr)
            {
                if (strSymbol.Contains("/"))
                {
                    blnIsFxRequest = true;
                }
                else 
                {
                    HCException.ThrowIfTrue(blnIsFxRequest,
                        "Mixed data type requests are not supported");
                }
            }
            return blnIsFxRequest;
        }

        public static IEnumerable<ITsEvent> GetFunctionFromColumn(
            List<ITsEvent> events,
            string strColumn)
        {
            var outEvents =
                new List<ITsEvent>();

            if (!events.Any())
            {
                return outEvents;
            }

            var binder = ReflectorCache.GetReflector(events.First().GetType());

            var strPropertyName = (from n in binder.GetPropertyNames()
                                   where n.Equals(strColumn)
                                   select n).First();

            foreach (ITsEvent timeSeriesEvent in events)
            {
                var dblValue =
                    ParserHelper.CastToDouble(
                        binder.GetPropertyValue(
                            timeSeriesEvent,
                            strPropertyName));
                if (!Double.IsNaN(dblValue))
                {
                    outEvents.Add(
                        new TsRow2D(
                            timeSeriesEvent.Time,
                            dblValue));
                }
            }
            return outEvents;
        }

        public static string GetResourceName(
            DateTime stardDate,
            DateTime endDate,
            string strSymbol,
            TsDataRequest dataRequest)
        {
            var localTsDataRequest = ClonerHelper.Clone(dataRequest);
            localTsDataRequest.StartTime = stardDate;
            localTsDataRequest.EndTime = endDate;
            localTsDataRequest.Symbols = strSymbol;
            return localTsDataRequest.Name;
        }

        public static string GetResourceName2(
            DateTime stardDate,
            DateTime endDate,
            string strSymbol,
            TsDataRequest dataRequest)
        {
            var localTsDataRequest = ClonerHelper.Clone(dataRequest);
            localTsDataRequest.StartTime = stardDate;
            localTsDataRequest.EndTime = endDate;
            localTsDataRequest.Symbols = strSymbol;
            return localTsDataRequest.Name;
        }

        public static string GetResourceName(
            DateTime stardDate,
            DateTime endDate,
            TsDataRequest dataRequest)
        {
            var localTsDataRequest = ClonerHelper.Clone(dataRequest);
            localTsDataRequest.StartTime = stardDate;
            localTsDataRequest.EndTime = endDate;
            return localTsDataRequest.Name;
        }
    }
}
