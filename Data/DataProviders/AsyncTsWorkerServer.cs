#region

using System;
using System.Collections.Generic;
using HC.Analytics.TimeSeries;
using HC.Core.Comunication.RequestResponseBased.Server;
using HC.Core.DynamicCompilation;
using HC.Core.Logging;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    public class AsyncTsWorkerServer : IDisposable
    {
        #region Properties

        public List<ITsEvent> TsEvents { get; private set; }
        public bool IsClientDisconnected { get; private set; }

        #endregion

        #region Members

        private TsDataRequest m_tsDataRequest;
        private string m_strRequestorName;

        #endregion

        public AsyncTsWorkerServer(
            TsDataRequest tsDataRequest,
            string strRequestorName)
        {
            m_tsDataRequest = tsDataRequest;
            m_strRequestorName = strRequestorName;
        }

        public void Work()
        {
            try
            {
                //Logger.Log("Server is requesting async events: " + m_tsDataRequest.Name);

                if(!ReqRespServer.ReqRespServerHeartBeat.IsClientConnected(m_strRequestorName))
                {
                    string strMessage = "Client [" + m_strRequestorName + "] is disconnected. Request [" +
                                        m_tsDataRequest.Name +
                                        " ] is not loaded.";
                    IsClientDisconnected = true;
                    Logger.Log(strMessage);
                    Console.WriteLine(strMessage);
                    return;
                }

                TsEvents = QuickTsDataProvider.GetTsEvents(m_tsDataRequest).TsEventsList;

                //if (TsEvents != null)
                //{
                //    Logger.Log("Server is finish loading async events [" + m_tsDataRequest.Name +
                //               "]. Vector size [" + TsEvents.Count + "]");
                //}

                lock (AsyncTsQueues.m_statsLock)
                {
                    //
                    // jobs done
                    //
                    int intJobsDone;
                    ReqRespServer.ReqRespServerHeartBeat.ProviderStats.TryGetIntValue(
                        EnumDataProvider.JobsProcessed,
                        out intJobsDone);
                    intJobsDone++;
                    ReqRespServer.ReqRespServerHeartBeat.ProviderStats.SetIntValue(
                        EnumDataProvider.JobsProcessed,
                        intJobsDone);
                }
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
        }

        public void Dispose()
        {
            TsEvents = null;
            m_tsDataRequest = null;
            m_strRequestorName = null;
        }
    }
}