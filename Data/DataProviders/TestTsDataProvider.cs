using System;
using System.Collections.Generic;
using System.Diagnostics;
using HC.Analytics.TimeSeries;
using HC.Core.DataStructures;
using HC.Core.DynamicCompilation;
using HC.Core.Exceptions;
using HC.Core.Threading.ProducerConsumerQueues;
using HC.Core.Threading.ProducerConsumerQueues.Support;
using HC.Utils.Basic.Data.Caches.TsCache;

namespace HC.Utils.Basic.Data.DataProviders
{
    public class TestTsDataProvider : ATsDataProvider
    {
        public static void TestProvider()
        {
            var serializerDb =
                TsCacheFactory.BuildSerializerCache(typeof(TestTsDataProvider));
            serializerDb.Clear();
            var queue = new ProducerConsumerQueue<StringWrapper>(50);
            int intVectorSize = 5000;
            queue.OnWork += dummy =>
                                {
                                    var evList = 
                                        new List<ITsEvent>();
                                    for (int i = 0; i < intVectorSize; i++)
                                    {
                                        evList.Add(new TestTsEvent
                                            {
                                                TestProp = Guid.NewGuid().ToString()
                                            });
                                    }
                                    serializerDb.Add(
                                        Guid.NewGuid().ToString(),
                                        evList);
                                };
            var taskList = new List<TaskWrapper>();
            int intImportSize = 500;
            for (int i = 0; i < intImportSize; i++)
            {
                taskList.Add(queue.EnqueueTask(null));
                //Thread.Sleep(100);
            }
            TaskWrapper.WaitAll(taskList.ToArray());
            if (serializerDb.Count != intVectorSize * intImportSize)
            {
                throw new HCException("Invalid values cached");
            }
            Debugger.Break();
            //while (true)
            //{
            //    Thread.Sleep(10);
            //}
        }

        protected override ITsEvents ExtractData(TsDataRequest tsDataRequest)
        {
            throw new NotImplementedException();
        }

        public override Type GetTsEventType()
        {
            return typeof(TestTsEvent);
        }
    }
}
