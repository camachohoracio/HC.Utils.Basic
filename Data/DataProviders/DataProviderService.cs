#region

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using HC.Analytics.TimeSeries;
using HC.Core;
using HC.Core.Comunication.RequestResponseBased.Server;
using HC.Core.Comunication.RequestResponseBased.Server.RequestHub;
using HC.Core.Distributed;
using HC.Core.Distributed.Controller;
using HC.Core.DynamicCompilation;
using HC.Core.Logging;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    public static class DataProviderService
    {
        #region Properties

        public static AsyncTsQueues AsyncTsQueues { get; private set; }
        public static bool IsConnected { get; private set; }

        #endregion

        #region Members

        private static string m_strServerName;
        private static readonly object m_serviceStartedLock = new object();
        private static DistController m_distController;
        private static readonly object m_distControllerLock = new object();
        private static DateTime m_prevLog;

        #endregion

        #region Constructors

        public static void Connect(
            string strServerName,
            int intPort)
        {
            try
            {
                if (!IsConnected)
                {
                    lock (m_serviceStartedLock)
                    {
                        if (!IsConnected)
                        {
                            string strMessage = "Starting: " + typeof (DataProviderService).Name +
                                                "...";
                            Console.WriteLine(strMessage);
                            Logger.Log(strMessage);
                            ReqRespService.Callbacks[
                                (int) EnumRequestType.Calc].OnGetObjectList += RequestCalc;
                            ReqRespService.Callbacks[
                                (int) EnumRequestType.DataProvider].OnGetObjectList += 
                                RequestFromDataProvider;

                            strServerName = strServerName.ToLower();
                            m_strServerName = strServerName;
                            AsyncTsQueues = new AsyncTsQueues(strServerName, intPort);
                            IsConnected = true;
                            strMessage = "Started: " + typeof (DataProviderService).Name;
                            Console.WriteLine(strMessage);
                            Logger.Log(strMessage);
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
        }

        #endregion

        #region Public

        private static List<object> RequestCalc(
            RequestDataMessage transferMessage)
        {
            try
            {
                DateTime logTime = DateTime.Now;
                string strMessage = typeof(DataProviderService).Name + " requested calc...";
                Verboser.WriteLine(strMessage);
                Logger.Log(strMessage);

                AsyncTsQueues.UpdateClientStatsJobsInProgress(
                    transferMessage.RequestorName,
                    true);
                AsyncTsQueues.UpdateClientStatsCalcsInProgress(
                    transferMessage.RequestorName,
                    true);
                //
                // this is a calc message. Send to distributed calc parent
                //
                var calcMessage = (ASelfDescribingClass)transferMessage.Request;
                
                if (m_distController == null)
                {
                    lock (m_distControllerLock)
                    {
                        if (m_distController == null)
                        {
                            m_distController = DistController.GetController(
                                EnumRequestType.Calc.ToString());
                            ReqRespServer.ReqRespServerHeartBeat.OnClientDisconnected += 
                                OnClientDisconnected;
                        }
                    }
                }
                calcMessage.SetStrValue(
                    EnumDistributed.RequestorName,
                    transferMessage.RequestorName);

                ASelfDescribingClass result = m_distController.DoWork(
                    calcMessage);
                
                if(result == null)
                {
                    string strJobId = calcMessage.GetStrValue(EnumDistributed.JobId);
                    if(m_distController.JobsDoneMap.ContainsKey(strJobId))
                    {
                        strMessage = "job[" + strJobId + "] already done";
                        Console.WriteLine(strMessage);
                        Logger.Log(strMessage);
                        return new List<object>(new object[]
                                        {
                                             EnumDistributed.AlreadyDone.ToString()
                                        });
                    }
                }

                AsyncTsQueues.UpdateClientStatsJobsDone(
                    transferMessage.RequestorName);
                AsyncTsQueues.UpdateClientStatsJobsInProgress(
                    transferMessage.RequestorName,
                    false);
                AsyncTsQueues.UpdateClientStatsCalcsInProgress(
                    transferMessage.RequestorName,
                    false);

                strMessage = "JobsDone calc. Time = " +
                                    (DateTime.Now - logTime).TotalSeconds;
                Console.WriteLine(strMessage);
                Logger.Log(strMessage);

                bool blnIsClientDisconnected;
                if (result != null &&
                    result.TryGetBlnValue(
                        EnumCalcCols.IsClientDisconnected,
                        out blnIsClientDisconnected))
                {
                    transferMessage.SetIsClientDisconnected(blnIsClientDisconnected);
                }

                return new List<object>(new[]
                                        {
                                             result
                                        });
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }

        private static void OnClientDisconnected(string strClientName)
        {
            while (m_distController == null)
            {
                const string strMessage = "Waiting to controller to be added...";
                Console.WriteLine(strMessage);
                Logger.Log(strMessage);
                Thread.Sleep(1000);
            }

            DistGuiHelper.PublishControllerLog(
                m_distController,
                "Worker [" + strClientName + "] is disconnected");
            m_distController.DistControllerToWorkerHeartBeat.RemoveJobsInProgressFromRequestor(strClientName);
        }


        /// <summary>
        ///   Request made via request/response service
        /// </summary>
        /// <param name = "transferMessage"></param>
        /// <returns></returns>
        private static List<object> RequestFromDataProvider(
            RequestDataMessage transferMessage)
        {
            string strMessage;
            if (!IsConnected)
            {
                //
                // make sure we are connected
                //
                while (!IsConnected)
                {
                    strMessage = typeof (DataProviderService).Name + " is not connected";
                    Console.WriteLine(strMessage);
                    Logger.Log(strMessage);
                    Thread.Sleep(1000);
                }
            }
            //
            // default, request ts events
            //
            var tsDataRequest = (TsDataRequest)transferMessage.Request;
            tsDataRequest.UseService = false;
            if (m_strServerName.Equals("local"))
            {
                return QuickTsDataProvider.GetTsEvents(tsDataRequest).TsEventsList.Cast<object>().ToList();
            }
            bool blnIsClientDisconnected;

            bool blnDoLog = (DateTime.Now - m_prevLog).TotalSeconds > 2;
            if (blnDoLog)
            {
                string strResourceName;
                if (tsDataRequest.DataProviderType == typeof(GenericTsDataProvider).Name)
                {
                    string strClassName = tsDataRequest.CustomParams[
                        EnumCalcCols.MethodClassName.ToString()].ToString();
                    string strMethodName = tsDataRequest.CustomParams[
                        EnumCalcCols.MethodName.ToString()].ToString();
                    strResourceName = 
                        strMethodName + "(.)" +
                        "[" + strClassName + "]";
                }
                else
                {
                    strResourceName =
                        "[" + tsDataRequest.DataProviderType + "][" +
                        tsDataRequest.Symbols + "]";
                }

                m_prevLog = DateTime.Now;

                strMessage = Environment.NewLine + "------------ Loading [" + strResourceName +
                             tsDataRequest.DataProviderType + "]...";
                Console.WriteLine(strMessage);
                Logger.Log(strMessage);
            }

            List<ITsEvent> tsEvents = AsyncTsQueues.GetTsEvents(
                tsDataRequest,
                transferMessage.RequestorName,
                out blnIsClientDisconnected);
            transferMessage.SetIsClientDisconnected(blnIsClientDisconnected);

            //if (blnDoLog)
            //{
            //    strMessage = "------------ " + typeof(DataProviderService).Name +
            //        " Done [" + tsDataRequest.DataProviderType + "[";
            //    Console.WriteLine(strMessage);
            //    Logger.Log(strMessage);
            //}
            if(tsEvents == null)
            {
                return new List<object>();
            }
            return tsEvents.Cast<object>().ToList();
        }

        #endregion
    }
}