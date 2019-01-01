#region

using System;
using System.Collections.Generic;
using System.Threading;
using HC.Analytics.TimeSeries;
using HC.Core;
using HC.Core.Cache;
using HC.Core.Distributed.Worker;
using HC.Core.DynamicCompilation;

#endregion

namespace HC.Utils.Basic.Calc
{
    public class TestCalc : ITsCalcWorker
    {
        #region Properties

        public List<ITsEvent> TsEvents { get; set; }
        public CacheDictionary<string, List<ITsEvent>> Cache { get; set; }
        public bool DoCache { get; set; }
        public ASelfDescribingClass Params { get; set; }
        public string Resource { get; set; }

        #endregion

        public void Work()
        {
            //
            // fake some work here
            //
            var rng = new Random();
            int intTimeWaited = rng.Next(100, 3000);
            Thread.Sleep(intTimeWaited);
            var selfDescribingTsEvent = new SelfDescribingTsEvent
                                            {
                                                Time = DateTime.Now
                                            };
            selfDescribingTsEvent.SetClassName(GetType().Name);
            selfDescribingTsEvent.SetIntValue("TimeWaited", intTimeWaited);
            TsEvents.Add(selfDescribingTsEvent);
        }


        public string GetResourceName()
        {
            return GetType().Name;
        }

        public virtual void GetCalcParams(TsDataRequest tsDataRequest)
        {
        }

        public virtual List<ITsEvent> LoadAllCalcs(TsDataRequest tsDataRequest)
        {
            return null;
        }

        public void Dispose()
        {
            if (TsEvents != null)
            {
                TsEvents.Clear();
                TsEvents = null;
            }
            if (Cache != null)
            {
                Cache.Clear();
                Cache = null;
            }
            if (Params != null)
            {
                Params.Dispose();
                Params = null;
            }
            Resource = null;
        }
    }
}

