#region

using System;
using System.Collections.Generic;
using HC.Core.Cache;
using HC.Core.DynamicCompilation;
using HC.Core.Threading.ProducerConsumerQueues.Support;

#endregion

namespace HC.Utils.Basic.Data.Caches.TsCache
{
    public interface ITsCache : ICache
    {
        string DefaultSubPath { get; set; }
        List<string> Keys { get; }
        List<ITsEvent> this[string strKey] { get; }
        List<ITsEvent> Get(List<string> keyList);
        object DeserializeDatabaseEntry(object value, bool compressItems);
        object DeserializeKey(object key);
        void Clear(DateTime dateTime);
        void Shrink(DateTime dateTime);
        List<string> GetKeysFromDate(
            DateTime dateTime);

        bool ContainsKey(
            string strKey);

        List<TaskWrapper> AddToTask(Dictionary<string, List<ITsEvent>> objs);
        List<TaskWrapper> AddToTask(Dictionary<string, ITsEvent> objs);
        TaskWrapper AddToTask(string oKey, ITsEvent oValue);
        TaskWrapper AddToTask(
            string strKey,
            List<ITsEvent> events);


        void Add(Dictionary<string, List<ITsEvent>> objs);
        void Add(Dictionary<string, ITsEvent> objs);
        void Add(string oKey, ITsEvent oValue);
        void Add(
            string strKey,
            List<ITsEvent> events);

        void Delete(string strKey);
        void Delete(List<string> strKeys);

        List<ITsEvent> Get(string strKey);
        string GetFileName(DateTime dateTime);
        string GetFileDir(DateTime dateTime);
        List<ITsEvent> GetAll(DateTime dateTime);
        List<ITsEvent> GetAll();
        List<ITsEvent> GetAll(string strQuery);
        Dictionary<string, List<ITsEvent>> GetAllMap(DateTime date);
        Dictionary<string, List<ITsEvent>> GetAllMap(string strQuery);
        int GetRowCount(DateTime dateTime);
        int GetRowCount(string strWhere);
        List<string> ContainsKeys(List<string> keys);
    }
}

