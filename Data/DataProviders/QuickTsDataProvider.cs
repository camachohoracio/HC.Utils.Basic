#region

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using HC.Analytics.TimeSeries;
using HC.Core.Comunication.RequestResponseBased.Client;
using HC.Core.Comunication.RequestResponseBased.Server.RequestHub;
using HC.Core.ConfigClasses;
using HC.Core.Distributed;
using HC.Core.DynamicCompilation;
using HC.Core.Logging;
using HC.Core.Reflection;
using HC.Core.Threading.Buffer;
using HC.Utils.Basic.Data.Caches.TsCache;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    public static class QuickTsDataProvider
    {

        #region Properties

        public static EfficientMemoryBuffer<string, ITsEvents> TsMemoryBuffer { get; set; }
        public static HashSet<string> IntradayProviders { get; private set; }

        #endregion

        private static int m_intTotalRequests;
        private static readonly string m_strIntradayServerName;
        private static readonly int m_intIntradayPort;

        #region Constructors

        static QuickTsDataProvider()
        {
            try
            {
                int intPoolCapacty = Basic.Config.GetPoolCapacity();
                TsMemoryBuffer =
                    new EfficientMemoryBuffer<string, ITsEvents>(
                        intPoolCapacty,
                        10);
                IntradayProviders = new HashSet<string>(
                    Core.Config.GetIntradayProviders());
                m_strIntradayServerName =
                    Core.Config.GetIntradayDataServerName();
                m_intIntradayPort =
                    Core.Config.GetIntradayReqRespPort();
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
        }

        #endregion

        public static ITsEvents GetTimeSeriesEventsFromPool(
            TsDataRequest tsDataRequest)
        {
            try
            {
                ITsEvents tsEvents;
                if (TsMemoryBuffer.TryGetValue(tsDataRequest.Name, out tsEvents))
                {
                    return tsEvents;
                }
                var tmpTsDataRequest = (TsDataRequest) tsDataRequest.Clone();
                tmpTsDataRequest.UsePool = false;
                tsEvents = GetTsEvents(tmpTsDataRequest);
                TsMemoryBuffer.Add(tsDataRequest.Name, tsEvents);
                return tsEvents;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return new TsEvents();
        }

        public static ITsEvents GetTsEvents(
            TsDataRequest tsDataRequest)
        {
            try
            {
                if (tsDataRequest.UsePool)
                {
                    return GetTimeSeriesEventsFromPool(tsDataRequest);
                }
                if (tsDataRequest.UseService &&
                    !DistConstants.IsServerMode)
                {
                    //
                    // request data remotely
                    //
                    tsDataRequest = (TsDataRequest)tsDataRequest.Clone();
                    tsDataRequest.UseService = false;
                    Interlocked.Increment(ref m_intTotalRequests);
                    string strRequestId =
                        HCConfig.ClientUniqueName + "_" +
                        m_intTotalRequests + "_" +
                        tsDataRequest.Name + "_" +
                        Guid.NewGuid().ToString();
                    ARequestResponseClient connection = null;
                    if (IntradayProviders.Contains(
                        tsDataRequest.DataProviderType.Trim()))
                    {
                        connection = GetIntradayConnection();
                    }
                    else if(tsDataRequest.DataProviderType.Trim().Equals(
                        typeof(GenericTsDataProvider).Name))
                    {
                        object objVal;
                        if(tsDataRequest.CustomParams.TryGetValue(
                            EnumCalcCols.MethodClassName.ToString(), 
                            out objVal))
                        {
                            string strClassName = objVal.ToString().Trim().Split('.').Last();
                            if(IntradayProviders.Contains(strClassName))
                            {
                                connection = GetIntradayConnection();
                            }
                        }
                    }
                    if(connection == null)
                    {
                        connection =
                            ARequestResponseClient.GetDefaultConnection();
                    }
                    List<object> ts = connection
                        .SendRequestAndGetResponse(
                            new RequestDataMessage
                                {
                                    CallbackSize = DistConstants.CALLBACK_SIZE,
                                    Id = strRequestId,
                                    IsAsync = true,
                                    Request = tsDataRequest,
                                    RequestType = EnumRequestType.DataProvider,
                                },
                            DistConstants.TIME_OUT_SECS);
                    ITsEvents tsEvs = new TsEvents
                               {
                                   TsEventsList = ts.Cast<ITsEvent>().ToList(),
                               };

                    if (!string.IsNullOrEmpty(tsDataRequest.Column))
                    {

                        tsEvs =
                            new TsFunction(
                                tsDataRequest.Column,
                                "Date",
                                tsDataRequest.Column,
                                new List<TsRow2D>(from n in tsEvs.TsEventsList
                                                  where !double.IsNaN(((TsRow2D)n).Fx)
                                                  select (TsRow2D) n));
                    }

                    return tsEvs;
                }
                if (tsDataRequest.UseService)
                {
                    tsDataRequest = (TsDataRequest)tsDataRequest.Clone();
                    tsDataRequest.UseService = false;
                }
                return GetTimeSeriesEventsFromDataProvider(tsDataRequest);
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
            return new TsEvents();
        }

        public static ARequestResponseClient GetIntradayConnection()
        {
            try
            {
                ARequestResponseClient connection;
                connection =
                    ARequestResponseClient.GetConnection(
                        m_strIntradayServerName,
                        m_intIntradayPort);
                return connection;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }


        public static ITsEvents GetTimeSeriesEventsFromDataProvider(
            TsDataRequest tsDataRequest)
        {
            try
            {
                //
                // use reflection in order to load data
                //
                Type providerType = TsDataProviderHelper.GetDataProviderType(
                    tsDataRequest.DataProviderType);

                IReflector dataProviderReflector = ReflectorCache.GetReflector(
                    providerType);
                using (IDataProvider dataProvider = (ATsDataProvider)dataProviderReflector.CreateInstance())
                {
                    if (dataProvider != null)
                    {
                        return dataProvider.LoadData(tsDataRequest);
                    }
                    return new TsEvents();
                }
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
            return new TsEvents();
        }

        public static void RemoveEvents(TsDataRequest tsDataRequest)
        {
            if (tsDataRequest.UseService)
            {
                GenericTsDataProvider.RunMethodDistributedViaService(
                    typeof(QuickTsDataProvider),
                    "RemoveEventsLocal",
                    new List<object>(new[] { tsDataRequest }));
            }
            else
            {
                RemoveEventsLocal(tsDataRequest);
            }
        }

        public static bool RemoveEventsLocal(TsDataRequest tsDataRequest)
        {
            try
            {
                ITsCache cache = TsCacheFactory.BuildSerializerCache(tsDataRequest);
                cache.Delete(tsDataRequest.Name);
                return true;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return false;
        }
    }
}
