#region

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using HC.Analytics.TimeSeries;
using HC.Core.Distributed;
using HC.Core.DynamicCompilation;
using HC.Core.Logging;
using HC.Core.Resources;
using HC.Core.Threading;
using HC.Core.Time;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    public abstract class ATsDataProvider : IDataProvider
    {
        #region Members

        private static readonly ConcurrentDictionary<string, int> m_mapDataProviderToRequests =
            new ConcurrentDictionary<string, int>();
        private static DateTime m_prevTime;

        #endregion

        #region Properties

        public IDataRequest DataRequest { get; set; }
        public DateTime TimeUsed { get; set; }
        public object Owner { get; set; }
        public bool HasChanged { get; set; }

        #endregion

        #region Public

        public virtual void Dispose()
        {
            DataRequest = null;
            Owner = null;
        }

        public virtual void Close()
        {
        }

        public ITsEvents LoadData(
            TsDataRequest tsDataRequest)
        {
            lock (LockObjectHelper.GetLockObject(
                GetType().Name + "_" +
                tsDataRequest.Name))
            {
                var startTime =
                    tsDataRequest.StartTime;
                var endTime =
                    tsDataRequest.EndTime;

                var startRequest = DateTime.Now;

                if((endTime - startTime).Ticks == 0)
                {
                    tsDataRequest.EndTime = DateHelper.GetEndOfDay(endTime);
                    tsDataRequest.StartTime = DateHelper.GetStartOfDay(startTime);
                }
                ITsEvents finalTsEvents =
                    ExtractData(tsDataRequest);

                if (finalTsEvents == null)
                {
                    return new TsEvents
                               {
                                   TsEventsList = new List<ITsEvent>()
                               };
                }

                if (finalTsEvents.TsEventsList == null)
                {
                    finalTsEvents.TsEventsList = new List<ITsEvent>();
                    return finalTsEvents;
                }

                //
                // load column data as a function
                //
                finalTsEvents = LoadColumnAsAFunction(tsDataRequest, finalTsEvents);

                //
                // sort time series events
                //
                var q = (from n in finalTsEvents.TsEventsList
                         where n != null
                         select n).ToList();
                finalTsEvents.TsEventsList = q;
                finalTsEvents.TsEventsList.Sort(
                    new TsEventComparator());

                List<ITsEvent> events = (from n in finalTsEvents.TsEventsList
                     where n.Time >= tsDataRequest.StartTime &&
                           n.Time <= tsDataRequest.EndTime
                     select n).ToList();

                //
                // filter by date
                //
                finalTsEvents.TsEventsList = events;

                LogResult(startRequest, tsDataRequest, events.Count);
                return finalTsEvents;
            }
        }

        private static ITsEvents LoadColumnAsAFunction(
            TsDataRequest tsDataRequest, 
            ITsEvents finalTsEvents)
        {
            try
            {
                if (!string.IsNullOrEmpty(tsDataRequest.Column))
                {
                    var functionEvents =
                        TsDataProviderHelper.GetFunctionFromColumn(
                            finalTsEvents.TsEventsList,
                            tsDataRequest.Column);
                    finalTsEvents =
                        new TsFunction(
                            tsDataRequest.Column,
                            "Date",
                            tsDataRequest.Column,
                            new List<TsRow2D>(from n in functionEvents
                                              select (TsRow2D) n));
                }
                return finalTsEvents;
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return new TsEvents();
        }

        private void LogResult(
            DateTime startRequest,
            TsDataRequest tsDataRequest,
            int intCount)
        {
            string strTypeName = GetType().Name;
            int intCounter;
            m_mapDataProviderToRequests.TryGetValue(strTypeName,
                                                    out intCounter);
            intCounter++;
            m_mapDataProviderToRequests[strTypeName] = intCounter;
            if ((DateTime.Now - m_prevTime).TotalSeconds > 5)
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
                strResourceName += "[" +
                                   DateHelper.ToDateString(tsDataRequest.StartTime) + "][" +
                                   DateHelper.ToDateString(tsDataRequest.EndTime) + "]";
                var strMessage =
                    Environment.NewLine +
                    "||| Done " + GetType().Name + " [" + intCount + "][" +
                    strResourceName + "]. Took [" +
                    Math.Round((DateTime.Now - startRequest).TotalSeconds,1) +
                    "] sec [" + intCounter + "] |||";

                Logger.Log(strMessage);
                Console.WriteLine(strMessage);
                m_prevTime = DateTime.Now;
            }
        }

        #endregion

        #region Abstract Methods

        protected abstract ITsEvents ExtractData(
            TsDataRequest tsDataRequest);
        public abstract Type GetTsEventType();

        #endregion
    }
}
