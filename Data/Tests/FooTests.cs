#region

using System;
using System.Collections.Generic;
using System.Linq;
using HC.Analytics.TimeSeries;
using HC.Core.ConfigClasses;
using HC.Core.DynamicCompilation;
using HC.Core.Helpers;
using HC.Core.Io;
using HC.Core.Io.KnownObjects;
using HC.Core.Io.KnownObjects.KnownTypes;
using HC.Core.Threading;
using HC.Utils.Basic.Data.Caches.TsCache;
using HC.Utils.Basic.Data.DataProviders;
using NUnit.Framework;

#endregion

namespace HC.Utils.Basic.Data.Tests
{
    public static class FooTests
    {
        [SetUp]
        public static void SetupTests()
        {
            HCConfig.SetConfigDir(@"C:\HC\Config");
            AssemblyCache.Initialize();
            KnownTypesCache.LoadKnownTypes();
            TsDataProviderHelper.LoadDataProvidersTypes();
            TestHelper.CopyFiles(
                @"C:\HC\bin\AssemblyCache\CustomByEnv",
                FileHelper.GetCurrentAssemblyPath());
        }

        [Test]
        public static void DoTest()
        {
            var request = new TsDataRequest
                {
                    Symbols = "FooSym",
                    StartTime = new DateTime(2000,1,1), // do not use default, it does not work
                    EndTime = DateTime.Today,
                    DataProviderType = typeof (FooTsDataPovider).Name
                };
            List<ITsEvent> eventList = QuickTsDataProvider.GetTsEvents(
                request).TsEventsList;

            ITsCache fooCache = TsCacheFactory.BuildSerializerCache(typeof (FooTsDataPovider));
            Assert.IsTrue(fooCache.ContainsKey(request.Name), "Cache item not found");
            List<ITsEvent> cachedItems = fooCache.Get(request.Name);
            Assert.IsTrue(cachedItems.Count == 2, "Invalid number of items");

            List<string> cachedCsvList = (from n in cachedItems select n.ToCsvString()).ToList();

            var fooItems = FooTsDataPovider.GetFooItems(
                request);

            CompareEvents(cachedCsvList, eventList);
            CompareEvents(cachedCsvList, fooItems);

            //
            // second comparison
            //
            eventList = QuickTsDataProvider.GetTsEvents(
                request).TsEventsList;
            CompareEvents(cachedCsvList, eventList);

            Console.WriteLine(eventList.Count);
            ThreadWorker.InvokeCancelAllThreads();
        }

        private static void CompareEvents(
            List<string> cachedCsvList, 
            List<ITsEvent> fooItems)
        {
            if (cachedCsvList == null)
            {
                throw new ArgumentNullException("cachedCsvList");
            }
            for (int i = 0; i < fooItems.Count; i++)
            {
                string strCsv = fooItems[i].ToCsvString();
                Assert.IsTrue(cachedCsvList.Contains(
                    strCsv), "Item not found [" +
                             strCsv + "]");
            }
        }
    }
}
