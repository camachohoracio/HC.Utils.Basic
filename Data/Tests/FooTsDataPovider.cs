#region

using System;
using System.Collections.Generic;
using HC.Analytics.TimeSeries;
using HC.Core.DynamicCompilation;
using HC.Utils.Basic.Data.Caches.TsCache;
using HC.Utils.Basic.Data.DataProviders;

#endregion

namespace HC.Utils.Basic.Data.Tests
{
    public class FooTsDataPovider : ATsDataProvider
    {
        protected override ITsEvents ExtractData(TsDataRequest tsDataRequest)
        {
            ITsCache cache = TsCacheFactory.BuildSerializerCache(typeof(FooTsDataPovider));
            if(cache.ContainsKey(tsDataRequest.Name))
            {
                List<ITsEvent> cachedItems = cache.Get(tsDataRequest.Name);
                return new TsEvents
                {
                    TsEventsList = cachedItems
                };
            }

            var tsEventsList = GetFooItems(tsDataRequest);

            cache.Add(tsDataRequest.Name,tsEventsList);

            return new TsEvents
                {
                    TsEventsList = tsEventsList
                };
        }

        public static List<ITsEvent> GetFooItems(TsDataRequest tsDataRequest)
        {
            var tsEventsList = new List<ITsEvent>(new[]
                {
                    new FooTsEvent
                        {
                            Symbol = tsDataRequest.Symbols + "_ASym",
                            Time = tsDataRequest.StartTime,
                            DblValue = 123,
                            ListStr = new List<string>(new[] {"a", "b"}),
                        },
                    new FooTsEvent
                        {
                            Symbol = tsDataRequest.Symbols + "_BSym",
                            Time = tsDataRequest.EndTime,
                            DblValue = 456,
                            ListStr = new List<string>(new[] {"c", "d"})
                        }
                });
            return tsEventsList;
        }

        public override Type GetTsEventType()
        {
            return typeof (FooTsEvent);
        }
    }
}
