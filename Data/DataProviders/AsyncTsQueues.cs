#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using HC.Analytics.TimeSeries;
using HC.Core.Comunication.RequestResponseBased;
using HC.Core.Comunication.RequestResponseBased.Server;
using HC.Core.Distributed;
using HC.Core.DynamicCompilation;
using HC.Core.Events;
using HC.Core.Logging;
using HC.Core.Threading;
using HC.Core.Threading.ProducerConsumerQueues;
using HC.Core.Threading.ProducerConsumerQueues.Support;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    public class AsyncTsQueues
    {
        #region Constants

        private const int THREAD_COUNT = 10;

        #endregion

        #region Members

        private readonly ConcurrentDictionary<string, IThreadedQueue<AsyncTsWorkerServer>> m_mapProviderToQueue;
        private readonly object m_queueLock = new object();
        public static readonly object m_statsLock = new object();
        private readonly List<ThreadWorker> m_queueLogThreads;

        #endregion

        #region Constructors

        public AsyncTsQueues(
            string strServerName, 
            int intPort)
        {
            try
            {
                DistConstants.m_strServerName = strServerName;
                DistConstants.m_intPort = intPort;
                m_queueLogThreads = new List<ThreadWorker>();
                if (!strServerName.ToLower().Equals("local"))
                {
                    m_mapProviderToQueue =
                        new ConcurrentDictionary<string, IThreadedQueue<AsyncTsWorkerServer>>();
                }
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
        }

        #endregion

        #region Public

        public List<ITsEvent> GetTsEvents(
            TsDataRequest tsDataRequest, 
            string strRequestorName,
            out bool blnIsClientDisconnected)
        {
            try
            {
                using (var asyncWorker = new AsyncTsWorkerServer(
                    tsDataRequest,
                    strRequestorName))
                {
                    IThreadedQueue<AsyncTsWorkerServer> currQueue;
                    ValidateQueue(tsDataRequest, out currQueue);

                    using (TaskWrapper currTask = currQueue.EnqueueTask(
                         asyncWorker))
                    {
                        lock (m_statsLock)
                        {
                            int intJobsInProgress;
                            ReqRespServer.ReqRespServerHeartBeat.ProviderStats.TryGetIntValue(
                                EnumDataProvider.JobsInProgress,
                                out intJobsInProgress);
                            intJobsInProgress++;
                            ReqRespServer.ReqRespServerHeartBeat.ProviderStats.SetIntValue(
                                EnumDataProvider.JobsInProgress,
                                intJobsInProgress);

                            int intQueueSize = DataProviderService.AsyncTsQueues.GetQueueSize();
                            ReqRespServer.ReqRespServerHeartBeat.ProviderStats.SetIntValue(
                                EnumDataProvider.QueueSize,
                                intQueueSize);

                        }
                        UpdateClientStatsJobsInProgress(strRequestorName, true);

                        currTask.Wait();
                        lock (m_statsLock)
                        {
                            //
                            // jobs in progress
                            //
                            int intJobsInProgress;
                            ReqRespServer.ReqRespServerHeartBeat.ProviderStats.TryGetIntValue(
                                EnumDataProvider.JobsInProgress,
                                out intJobsInProgress);
                            intJobsInProgress--;
                            ReqRespServer.ReqRespServerHeartBeat.ProviderStats.SetIntValue(
                                EnumDataProvider.JobsInProgress,
                                intJobsInProgress);

                            //
                            // jobs done
                            //
                            int intJobsDone;
                            ReqRespServer.ReqRespServerHeartBeat.ProviderStats.TryGetIntValue(
                                EnumDataProvider.JobsDone,
                                out intJobsDone);
                            intJobsDone++;
                            ReqRespServer.ReqRespServerHeartBeat.ProviderStats.SetIntValue(
                                EnumDataProvider.JobsDone,
                                intJobsDone);
                        }

                        UpdateClientStatsJobsInProgress(strRequestorName, false);
                        UpdateClientStatsJobsDone(strRequestorName);
                        blnIsClientDisconnected = asyncWorker.IsClientDisconnected;
                        List<ITsEvent> tsEvents = asyncWorker.TsEvents.ToList();
                        return tsEvents;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
            blnIsClientDisconnected = false;
            return new List<ITsEvent>();
        }

        public static void UpdateClientStatsJobsDone(
            string strRequestorName)
        {
            lock (m_statsLock)
            {
                SelfDescribingClass clientStats;
                if (ReqRespServer.ReqRespServerHeartBeat.MapClientToPingTime.TryGetValue(
                    strRequestorName, out clientStats))
                {
                    int intJobsDone;
                    clientStats.TryGetIntValue(
                        EnumDataProvider.JobsDone,
                        out intJobsDone);
                    intJobsDone++;
                    clientStats.SetIntValue(
                        EnumDataProvider.JobsDone,
                        intJobsDone);
                }
            }
        }

        public static void UpdateClientStatsJobsInProgress(
            string strRequestorName,
            bool blnIncrease)
        {
            lock (m_statsLock)
            {
                SelfDescribingClass clientStats;
                if (ReqRespServer.ReqRespServerHeartBeat.MapClientToPingTime.TryGetValue(
                    strRequestorName, out clientStats))
                {
                    int intJobsInProgress;
                    clientStats.TryGetIntValue(
                        EnumDataProvider.JobsInProgress,
                        out intJobsInProgress);
                    if (blnIncrease)
                    {
                        intJobsInProgress++;
                    }
                    else
                    {
                        intJobsInProgress--;
                    }
                    clientStats.SetIntValue(
                        EnumDataProvider.JobsInProgress,
                        intJobsInProgress);
                }
            }
        }

        public static void UpdateClientStatsCalcsInProgress(
            string strRequestorName,
            bool blnIncrease)
        {
            lock (m_statsLock)
            {
                SelfDescribingClass clientStats;
                if (ReqRespServer.ReqRespServerHeartBeat.MapClientToPingTime.TryGetValue(
                    strRequestorName, out clientStats))
                {
                    int intJobsInProgress;
                    clientStats.TryGetIntValue(
                        EnumDataProvider.CalcsInProgress,
                        out intJobsInProgress);
                    if (blnIncrease)
                    {
                        intJobsInProgress++;
                    }
                    else
                    {
                        intJobsInProgress--;
                    }
                    clientStats.SetIntValue(
                        EnumDataProvider.CalcsInProgress,
                        intJobsInProgress);
                }
            }
        }

        public int GetQueueSize()
        {
            int intQueueSize = 0;
            foreach (IThreadedQueue<AsyncTsWorkerServer> threadedQueue in m_mapProviderToQueue.Values)
            {
                intQueueSize += threadedQueue.QueueSize;
            }
            return intQueueSize;
        }

        private void ValidateQueue(
            TsDataRequest tsDataRequest,
            out IThreadedQueue<AsyncTsWorkerServer> currQueue)
        {
            string strDataProvider = tsDataRequest.DataProviderType;
            if(!m_mapProviderToQueue.TryGetValue(
                strDataProvider,
                out currQueue))
            {
                lock(m_queueLock)
                {
                    if (!m_mapProviderToQueue.TryGetValue(
                        strDataProvider,
                        out currQueue))
                    {
                        int intThreads;
                        if (!DataProviderConstants.m_mapDataProviderToQueueSize.TryGetValue(
                            strDataProvider, out intThreads))
                        {
                            intThreads = THREAD_COUNT;
                        }
                        currQueue = new ProducerConsumerQueue<AsyncTsWorkerServer>(intThreads);
                        currQueue.OnWork += QueueOnWork;
                        m_mapProviderToQueue[strDataProvider] = currQueue;
                        Logger.Log("Loaded queue [" + strDataProvider + "] with " +
                                   THREAD_COUNT + " threads");
                        m_mapProviderToQueue[strDataProvider] = currQueue;
                        var worker = new ThreadWorker();
                        var queue = currQueue;
                        worker.OnExecute += () => LogQueue(strDataProvider,
                            DistConstants.m_strServerName,
                            DistConstants.m_intPort,
                            queue);
                        worker.Work();
                        m_queueLogThreads.Add(worker);
                    }
                }
            }
        }

        #endregion

        #region Private

        private static void LogQueue(
            string strQueueName,
            string strServerName,
            int intPort,
            IThreadedQueue<AsyncTsWorkerServer> queue)
        {
            try
            {
                while (true)
                {
                    try
                    {
                        var selfDescrClass = new SelfDescribingClass();
                        selfDescrClass.SetClassName(EnumDistributedGui.DataProviders.ToString() + "_queues");
                        selfDescrClass.SetStrValue("Queue", strQueueName);
                        selfDescrClass.SetIntValue("Threads", queue.Threads);
                        selfDescrClass.SetIntValue("Size", queue.QueueSize);
                        selfDescrClass.SetIntValue("InProgress", queue.TasksInProgress);
                        selfDescrClass.SetIntValue("Done", queue.TasksDone);
                        selfDescrClass.SetDateValue("Time", DateTime.Now);

                        LiveGuiPublisherEvent.PublishGrid(
                            EnumReqResp.Admin.ToString(),
                            EnumReqResp.RequestResponse.ToString() + "_" +
                                strServerName + "_" +
                                intPort,
                            EnumDistributedGui.DataProviders.ToString(),
                            Core.ConfigClasses.HCConfig.ClientUniqueName + strQueueName,
                            selfDescrClass,
                            0,
                            false);
                    }
                    catch (Exception ex)
                    {
                        Logger.Log(ex);
                    }
                    Thread.Sleep(5000);
                }
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
        }

        private static void QueueOnWork(AsyncTsWorkerServer asyncTsWorkerServer)
        {
            try
            {
                asyncTsWorkerServer.Work();
            }
            catch (Exception ex)
            {
                Logger.Log(ex);
            }
        }

        #endregion
    }
}
