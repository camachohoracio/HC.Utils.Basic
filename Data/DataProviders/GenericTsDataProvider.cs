#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using HC.Analytics.TimeSeries;
using HC.Core;
using HC.Core.Comunication;
using HC.Core.Comunication.RequestResponseBased;
using HC.Core.DataStructures;
using HC.Core.Distributed;
using HC.Core.DynamicCompilation;
using HC.Core.Events;
using HC.Core.Logging;
using HC.Core.Threading;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    public class GenericTsDataProvider : ATsDataProvider
    {

        #region Members

        private static readonly ConcurrentDictionary<string, ProviderCounterItem> m_methodCounter =
            new ConcurrentDictionary<string, ProviderCounterItem>();
        private static readonly ConcurrentDictionary<string, object> m_methodCounterChanges =
            new ConcurrentDictionary<string, object>();
        private static readonly object m_counterLock = new object();
        private static ThreadWorker m_logWorker;
        private DateTime m_prevLog;
        private static bool m_blnInitialized;
        private static readonly object m_lockObj = new object();

        #endregion

        #region Constructor

        static GenericTsDataProvider()
        {
            try
            {
                LoadLogWorker();
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
        }

        public static void Initialize()
        {
            if (!m_blnInitialized)
            {
                lock (m_lockObj)
                {
                    if (!m_blnInitialized)
                    {
                        ProviderEvents.OnRunMethodDistributedViaService += RunMethodDistributedViaService;
                        m_blnInitialized = true;
                    }
                }
            }
        }

        #endregion

        private static void LoadLogWorker()
        {
            m_logWorker = new ThreadWorker();
            m_logWorker.OnExecute += () =>
            {
                while (true)
                {
                    try
                    {
                        if (!string.IsNullOrEmpty(DistConstants.m_strServerName))
                        {
                            var methodCounterArr = m_methodCounterChanges.ToArray();
                            for (int i = 0; i < methodCounterArr.Length; i++)
                            {
                                var kvp = methodCounterArr[i];
                                ProviderCounterItem kvpCounter;
                                if (!m_methodCounter.TryGetValue(kvp.Key, out kvpCounter))
                                {
                                    continue;
                                }
                                var selfDescrClass = new SelfDescribingClass();
                                selfDescrClass.SetClassName(typeof(GenericTsDataProvider).Name + "_queues");
                                selfDescrClass.SetStrValue("Method", kvp.Key);
                                selfDescrClass.SetIntValue("ToDo", kvpCounter.Todo);
                                selfDescrClass.SetIntValue("Done", kvpCounter.Done);
                                selfDescrClass.SetDateValue("Time", DateTime.Now);

                                LiveGuiPublisherEvent.PublishGrid(
                                    EnumReqResp.Admin.ToString(),
                                    EnumReqResp.RequestResponse.ToString() + "_" +
                                    DistConstants.m_strServerName + "_" +
                                    DistConstants.m_intPort,
                                    typeof(GenericTsDataProvider).Name,
                                    kvp.Key,
                                    selfDescrClass,
                                    0,
                                    false);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Logger.Log(ex);
                    }
                    m_methodCounterChanges.Clear();
                    Thread.Sleep(5000);
                }
            };
            m_logWorker.Work();
        }

        public static object RunMethodDistributedViaService(
            Type classType,
            string strMethodName,
            List<object> parameters)
        {
            return RunMethod(
                classType,
                strMethodName,
                parameters,
                true);
        }


        public static object RunMethod(
            Type classType,
            string strMethodName,
            List<object> parameters,
            bool blnUseService)
        {
            try
            {
                //if (NetworkHelper.IsADistWorkerConnected && blnUseService)
                //{
                   // throw new HCException("A worker cannot call a provider. Posible deadlocks");
                //}

                ASelfDescribingClass calcParams = ExecuteMethodCalc.GetCalcParams(
                    parameters,
                    strMethodName,
                    classType);
                var paramsMap = new SerializableDictionary<string, object>();
                List<string> props = calcParams.GetAllPropertyNames();
                for (int i = 0; i < props.Count; i++)
                {
                    object currValue;
                    calcParams.TryGetValueFromAnyProperty(props[i], out currValue);
                    paramsMap[props[i]] = (currValue);
                }

                List<ITsEvent> results = QuickTsDataProvider.GetTsEvents(
                    new TsDataRequest
                        {
                            DataProviderType = typeof(GenericTsDataProvider).Name,
                            CustomParams = paramsMap,
                            StartTime = new DateTime(),
                            EndTime = DateTime.Now,
                            UseService = blnUseService
                        }).TsEventsList;

                if (results != null &&
                    results.Count > 0)
                {
                    var resultObj = (ASelfDescribingClass)results[0];
                    object result;
                    resultObj.TryGetObjValue(EnumCalcCols.Result,
                                             out result);
                    return result;
                }
                return null;
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }

        protected override ITsEvents ExtractData(TsDataRequest tsDataRequest)
        {
            try
            {

                ASelfDescribingClass selfDescribingClass = new SelfDescribingClass();
                foreach (var kvp in tsDataRequest.CustomParams)
                {
                    selfDescribingClass.SetValueToDictByType(kvp.Key, kvp.Value);
                }

                string strClassName =
                    selfDescribingClass.GetStrValue(
                        EnumCalcCols.MethodClassName);
                string strAssemblyName =
                    selfDescribingClass.GetStrValue(
                        EnumCalcCols.MethodAssemblyName).ToLower();

                string strMethodName = selfDescribingClass.GetStrValue(EnumCalcCols.MethodName);
                string strMethodDescr = strAssemblyName + "." +
                    strClassName + "." +
                    strMethodName;

                bool blnDoLog = (DateTime.Now - m_prevLog).TotalSeconds > 3;
                if (blnDoLog)
                {
                    m_prevLog = DateTime.Now;
                    //string strMessage = "***In progress->" +
                    //                    typeof (GenericTsDataProvider).Name + " [" +
                    //                    strMethodDescr + "]";
                    //Console.WriteLine(strMessage);
                    //Logger.Log(strMessage);
                }

                lock (m_counterLock)
                {
                    ProviderCounterItem kvpCounter;
                    if (!m_methodCounter.TryGetValue(
                        strMethodDescr, out kvpCounter))
                    {
                        kvpCounter = new ProviderCounterItem();
                        m_methodCounter[strMethodDescr] = kvpCounter;
                    }
                    kvpCounter.Todo++;
                    m_methodCounterChanges[strMethodDescr] = null;
                }

                string strError;
                object result = ExecuteMethodCalc.RunMethodLocally(
                    selfDescribingClass,
                    out strError);

                lock (m_counterLock)
                {
                    ProviderCounterItem kvpCounter;
                    if (!m_methodCounter.TryGetValue(
                        strMethodDescr, out kvpCounter))
                    {
                        kvpCounter = new ProviderCounterItem();
                        m_methodCounter[strMethodDescr] = kvpCounter;
                    }
                    kvpCounter.Done++;
                    kvpCounter.Todo--;
                    m_methodCounterChanges[strMethodDescr] = null;
                }

                //if (result == null)
                //{
                //    throw new HCException("Null result");
                //}

                var resultTsEv = new SelfDescribingTsEvent(GetType().Name)
                {
                    Time = tsDataRequest.StartTime
                };
                resultTsEv.SetObjValueToDict(
                    EnumCalcCols.Result,
                    result);

                if (!string.IsNullOrEmpty(strError))
                {
                    resultTsEv.SetStrValue(
                        EnumCalcCols.Error,
                        strError);
                }

                //if (blnDoLog)
                //{
                //    string strMessage = "***Done->" +
                //                        typeof (GenericTsDataProvider).Name + " [" +
                //                        strMethodDescr + "]";
                //    Console.WriteLine(strMessage);
                //    Logger.Log(strMessage);
                //}

                return new TsEvents
                {
                    TsEventsList = new List<ITsEvent>(new[]
                                                                 {
                                                                     resultTsEv
                                                                 })
                };
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
            return new TsEvents();
        }

        public override Type GetTsEventType()
        {
            return typeof (SelfDescribingTsEvent);
        }
    }
}