#region

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using HC.Analytics.TimeSeries;
using HC.Core.Exceptions;
using HC.Core.Helpers;
using HC.Core.Time;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    [Serializable]
    public class TsDataSubscription
    {
        #region Members

        private readonly List<TsDataRequest> m_tsDataRequestList;
        private List<KeyValuePair<DatePeriod, double>> m_playSpeeds;

        #endregion

        #region Constructors
        
        public TsDataSubscription()
        {
            m_tsDataRequestList = new List<TsDataRequest>();
        }

        #endregion

        #region Public

        public List<TsDataRequest> GetRequestList()
        {
            return m_tsDataRequestList;
        }

        public void AddDataRequest(
            string strName,
            TsDataRequest tsDataRequest,
            PublishTsDelegate publishTsDelegate)
        {
            tsDataRequest.AddPublishTsDelegate(strName, publishTsDelegate);
            //
            // make sure the request is not added twice
            //
            foreach (TsDataRequest currentTsDataRequest in m_tsDataRequestList)
            {
                if (currentTsDataRequest.ToString().Equals(tsDataRequest.ToString()))
                {
                    currentTsDataRequest.AddPublishTsDelegate(strName, publishTsDelegate);
                    //
                    // avoid adding duplicated requests
                    //
                    return;
                }
            }

            m_tsDataRequestList.Add(
                tsDataRequest);
        }


        public List<DatePeriod> GetDatePeriods()
        {
            var datePeriodsMap =
                new Dictionary<string, DatePeriod>();
            foreach (TsDataRequest tsDataRequest in m_tsDataRequestList)
            {
                var datePeriods = DateHelper.GetDatePeriodList(
                    tsDataRequest.StartTime,
                    tsDataRequest.EndTime,
                    true);
                foreach (DatePeriod requestedPeriod in datePeriods)
                {
                    var strDate = DateHelper.ToDateString(requestedPeriod.StartDate);
                    DatePeriod mappedDatePeriod;
                    if (!datePeriodsMap.TryGetValue(
                        strDate,
                        out mappedDatePeriod))
                    {
                        datePeriodsMap[strDate] = requestedPeriod;
                    }
                    else
                    {
                        var startTime = mappedDatePeriod.StartDate;
                        var endTime = mappedDatePeriod.EndDate;
                        var blnNewDateFound = false;
                        if (mappedDatePeriod.StartDate > requestedPeriod.StartDate)
                        {
                            startTime = requestedPeriod.StartDate;
                            blnNewDateFound = true;
                        }
                        if (mappedDatePeriod.EndDate < requestedPeriod.EndDate)
                        {
                            endTime = requestedPeriod.EndDate;
                            blnNewDateFound = true;
                        }
                        if (blnNewDateFound)
                        {
                            //
                            // replace time period
                            //
                            datePeriodsMap[strDate] = new DatePeriod(
                                startTime,
                                endTime);
                        }
                    }
                }
            }

            var sorteDateList = datePeriodsMap.Values.ToList();
            sorteDateList.Sort(new DatePeriod());
            return sorteDateList;
        }

        public static TsDataSubscription MergeTsDataSubscriptions(
            TsDataSubscription tsDataSubscription1,
            TsDataSubscription tsDataSubscription2)
        {
            var outTsDataSubscription =
                new TsDataSubscription();
            var tsDataRequestValidator =
                new Dictionary<string, TsDataRequest>();
            MergeCallbackPointers(tsDataSubscription1,
                                  outTsDataSubscription,
                                  tsDataRequestValidator);
            MergeCallbackPointers(tsDataSubscription2,
                                  outTsDataSubscription,
                                  tsDataRequestValidator);
            return outTsDataSubscription;
        }

        private static void MergeCallbackPointers(
            TsDataSubscription inTsDataSubscription,
            TsDataSubscription outTsDataSubscription,
            Dictionary<string, TsDataRequest> tsDataRequestValidator)
        {
            foreach (TsDataRequest tsDataRequest in
                inTsDataSubscription.m_tsDataRequestList)
            {
                TsDataRequest currTsDataRequest;
                if (!tsDataRequestValidator.TryGetValue(tsDataRequest.Name, out currTsDataRequest))
                {
                    currTsDataRequest = ClonerHelper.Clone(tsDataRequest);
                    tsDataRequestValidator[tsDataRequest.Name] = currTsDataRequest;

                    outTsDataSubscription.AddDataRequest(
                        string.Empty,
                        currTsDataRequest,
                        null);
                }
                //
                // add callbacks
                //
                var publishTsDelegates = tsDataRequest.GetTsEventPublishList();
                if (publishTsDelegates != null)
                {
                    foreach (var kvp in publishTsDelegates)
                    {
                        currTsDataRequest.AddPublishTsDelegate(
                            kvp.Key,
                            kvp.Value);
                    }
                }
            }
        }

        public double GetPlaySpeed(
            DateTime startDate,
            DateTime endTime)
        {
            if (m_playSpeeds == null)
            {
                m_playSpeeds = new List<KeyValuePair<DatePeriod, double>>();
                var q = from n in m_tsDataRequestList
                        where n.PlaySpeed > 0
                        select n;

                if (q.Any())
                {
                    foreach (TsDataRequest tsDataRequest in q)
                    {
                        m_playSpeeds.Add(
                            new KeyValuePair<DatePeriod, double>(
                                new DatePeriod(tsDataRequest.StartTime,
                                               tsDataRequest.EndTime),
                                tsDataRequest.PlaySpeed));
                    }
                }
            }
            else if (m_playSpeeds.Count == 0)
            {
                return 1.0;
            }
            var p = from n in m_playSpeeds
                    where startDate >= n.Key.StartDate &&
                          endTime <= n.Key.EndDate
                    select n.Value;
            if (!p.Any())
            {
                return 1.0;
            }
            HCException.ThrowIfTrue(p.Count() > 1,
                "Duplicated play speed.");
            
            return p.First();
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            foreach (TsDataRequest tsDataRequest in m_tsDataRequestList)
            {
                sb.Append(tsDataRequest.Name);
            }
            return sb.ToString();
        }

        #endregion
    }
}
