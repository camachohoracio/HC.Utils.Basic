#region

using System;
using System.Collections.Generic;
using System.Linq;
using HC.Analytics.TimeSeries;
using HC.Core.DynamicCompilation;
using HC.Core.Exceptions;
using HC.Core.Helpers;
using HC.Core.Time;
using HC.Utils.Basic.Data.Caches.TsCache;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    public abstract class ASimpleTsDataProvider<T> :
        ATsDataProvider where T : ITsEvent
    {
        #region Public

        public override Type GetTsEventType()
        {
            return typeof(T);
        }

        protected override ITsEvents ExtractData(
            TsDataRequest tsDataRequest)
        {
            //
            // clean data request
            //
            tsDataRequest = ClonerHelper.Clone(tsDataRequest);
            var startTime = DateHelper.GetStartOfDay(tsDataRequest.StartTime);
            var endTime = DateHelper.GetEndOfDay(tsDataRequest.EndTime);

            if ((endTime - startTime).TotalSeconds == 0)
            {
                throw new HCException("End time not valid [" + tsDataRequest.Name + "]");
            }

            var finalTsEvents = new TsEvents();

            var datePairs =
                DateHelper.GetDatePeriodList(
                    startTime,
                    endTime,
                    true);

            foreach (DatePeriod datePair in datePairs)
            {
                if(datePair.StartDate.Year < 1970)
                {
                    continue;
                }
                if (datePair.StartDate.Year > DateTime.Today.Year + 50)
                {
                    continue;
                }

                //
                // iterate each symbol
                //
                List<ITsEvent> filteredEvents =
                    LoadDataPerDay(
                        datePair.StartDate,
                        datePair.EndDate,
                        tsDataRequest);

                //
                // filter events by date
                //
                finalTsEvents.TsEventsList.AddRange(filteredEvents);
            }

            finalTsEvents.TsEventsList.Sort(
                new TsEventComparator());
            return finalTsEvents;
        }

        #endregion

        #region Private

        private static List<ITsEvent> LoadDataPerDay(
            DateTime startTime,
            DateTime endTime,
            TsDataRequest tsDataRequest)
        {
            if(startTime.Year < 1950)
            {
                startTime = new DateTime(1950,1,1);
            }
            //
            // get news data for entire days
            //
            startTime = DateHelper.GetStartOfDay(startTime);
            endTime = DateHelper.GetEndOfDay(endTime);

            //
            // get events by ric from the serializer
            //
            ITsCache serializerDb =
                TsCacheFactory.BuildSerializerCache(tsDataRequest);
            var unfilteredEvents = new List<ITsEvent>();
            
            string[] strSymbols = tsDataRequest.SymbolArr;
            if(strSymbols == null ||
                strSymbols.Length == 0)
            {
                //
                // load everything if no symbol provided
                //
                unfilteredEvents = serializerDb.GetAll(startTime);


            }
            else
            {
                //
                // iterate each symbol
                //
                var currTsDataRequest = (TsDataRequest)tsDataRequest.Clone();
                var requests = new List<string>();
                foreach (string strSymbol in strSymbols)
                {
                    currTsDataRequest.Symbols = strSymbol;
                    var strResourceName =
                        TsDataProviderHelper.GetResourceName(
                            startTime,
                            endTime,
                            tsDataRequest);
                    requests.Add(strResourceName);
                }

                //
                // load requests
                //
                unfilteredEvents.AddRange(
                    serializerDb.Get(requests));
            }

            if(unfilteredEvents != null &&
                unfilteredEvents.Count > 0)
            {
                List<ITsEvent> outOfDate = (from n in unfilteredEvents
                                            where n.Time < startTime
                                            select n).ToList();

                if (outOfDate.Count > 0)
                {
                    for (int i = 0; i < unfilteredEvents.Count; i++)
                    {
                        unfilteredEvents[i].Time = startTime;
                    }
                }
            }

            //
            // filter events by symbol
            //
            if (tsDataRequest.CurrencyArr == null ||
                tsDataRequest.CurrencyArr.Length == 0)
            {
                return unfilteredEvents;
            }

            //
            // filter events by currency symbol
            //
            var filteredEvents =
                new List<ITsEvent>(
                    from n in unfilteredEvents
                    where n.Time >= startTime &&
                          n.Time <= endTime
                    select n);

            return filteredEvents;
        }

        #endregion
    }
}
