#region

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using HC.Analytics.TimeSeries;
using HC.Core.DynamicCompilation;
using HC.Core.Events;
using HC.Core.Exceptions;
using HC.Core.Io;
using HC.Core.Logging;
using HC.Core.Reflection;
using HC.Core.Resources;
using HC.Core.Threading.ProducerConsumerQueues.Support;
using HC.Core.Time;
using HC.Core.Zip;
using HC.Utils.Basic.Data.DataProviders;

#endregion

namespace HC.Utils.Basic.Data.Caches.TsCache
{
    public class TsCsvCache : ITsCache
    {
        #region Members

        private readonly Type m_dataProviderProviderType;
        protected string m_strPath;
        private string[] m_strTitles;
        private bool m_blnZipFile;

        #endregion

        #region Properties

        //public bool IsSelfDescribingClass { get; set; }

        public virtual List<string> Keys
        {
            get
            {
                var allFiles =
                    FileHelper.GetFileList(m_strPath);
                var selectedFiles = GetSelectedFiles(allFiles);
                return selectedFiles;
            }
        }

        public virtual int Count
        {
            get { return FileHelper.GetFileList(m_strPath).Count; }
        }

        public IDataRequest DataRequest { get; set; }

        public DateTime TimeUsed { get; set; }

        public object Owner { get; set; }

        public bool HasChanged { get; set; }

        public string DefaultSubPath { get; set; }

        public void Close()
        {
        }

        public bool CompressItems
        {
            get { return m_blnZipFile; }
            set { m_blnZipFile = value; }
        }

        #endregion

        #region Constructor

        public TsCsvCache(
            string strPath,
            Type dataProviderProviderType,
            bool blnZipFile,
            string[] strTitles)
        {
            m_strPath = strPath;
            m_dataProviderProviderType = dataProviderProviderType;
            m_blnZipFile = blnZipFile;
            m_strTitles = strTitles;
        }

        #endregion

        #region Public Methods

        public List<ITsEvent> this[string strKey]
        {
            get { return Get(strKey); }
        }

        public virtual void Dispose()
        {
            HC.Core.EventHandlerHelper.RemoveAllEventHandlers(this);
        }

        public virtual List<ITsEvent> Get(List<string> keyList)
        {
            throw new NotImplementedException();
        }

        public virtual TaskWrapper AddToTask(string oKey, ITsEvent oValue)
        {
            throw new NotImplementedException();
        }

        public virtual void Add(object oKey, object oValue)
        {
            throw new NotImplementedException();
        }

        public bool ContainsKey(object oKey)
        {
            return ContainsKey(oKey as string);
        }

        public IEnumerator<KeyValuePair<object, object>> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public object DeserializeDatabaseEntry(object value, bool compressItems)
        {
            throw new NotImplementedException();
        }

        public object DeserializeKey(object key)
        {
            return key as string;
        }

        public void Delete(object oKey)
        {
            Delete(oKey as string);
        }

        public virtual void Clear()
        {
            if (DirectoryHelper.Exists(m_strPath))
            {
                FileHelper.DeleteAllFiles(m_strPath, true);
            }
        }
        
        public virtual void Clear(DateTime dateTime) { }
        public virtual void Shrink(DateTime dateTime)
        {
            throw new NotImplementedException();
        }

        public object Get(object oKey)
        {
            return Get(oKey as string);
        }

        public void Update(object oKey, object oValue)
        {
            throw new NotImplementedException();
        }

        private static List<string> GetSelectedFiles(List<string> allFiles)
        {
            var selectedFiles = new List<string>();
            foreach (string strFileName in allFiles)
            {
                var fi = new FileInfo(strFileName);
                if (FileHelper.GetExtension(strFileName).ToLower().Equals(".zip") ||
                    FileHelper.GetExtension(strFileName).ToLower().Equals(".csv"))
                {
                    var strKey = fi.Name
                        .Replace(".zip", "")
                        .Replace(".Zip", "")
                        .Replace(".ZIP", "")
                        .Replace(".csv", "")
                        .Replace(".Csv", "")
                        .Replace(".CSV", "");

                    selectedFiles.Add(strKey);
                }
            }
            return selectedFiles;
        }

        public virtual List<string> GetKeysFromDate(
            DateTime startDateTime)
        {
            var strDateString = DateHelper.ToDateString(
                startDateTime);
            var strDir = Path.Combine(m_strPath,
                                      strDateString);
            if (DirectoryHelper.Exists(strDir))
            {
                List<string> allFiles =
                    FileHelper.GetFileList(
                    strDir, 
                    false,
                    false);
                return allFiles;
            }
            return new List<string>();
        }

        public virtual bool ContainsKey(
            string strKey)
        {
            string strFileName;

            if (m_blnZipFile)
            {
                strFileName = GetZipFileName(strKey);
            }
            else
            {
                strFileName = GetCsvFileName(strKey);
            }

            return FileHelper.Exists(strFileName);
        }

        public virtual List<TaskWrapper> AddToTask(Dictionary<string, List<ITsEvent>> objs)
        {
            throw new NotImplementedException();
        }

        public List<TaskWrapper> AddToTask(Dictionary<string, ITsEvent> objs)
        {
            try
            {
                var mapKeyToTsEvents = new Dictionary<string, List<ITsEvent>>();
                foreach (var kvp in objs)
                {
                    try
                    {
                        if(string.IsNullOrEmpty(kvp.Key))
                        {
                            continue;
                        }
                        ITsEvent ts = kvp.Value;
                        mapKeyToTsEvents[kvp.Key] = new List<ITsEvent>
                                                        {
                                                            ts
                                                        };
                    }
                    catch(Exception ex)
                    {
                        Logger.Log(ex);
                    }
                }
                return AddToTask(mapKeyToTsEvents);
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
            return null;
        }

        public virtual TaskWrapper AddToTask(
            string strKey,
            List<ITsEvent> events)
        {
            var strCsvFileName = GetCsvFileName(
                strKey);

            var startLogTime = DateTime.Now;

            if (FileHelper.Exists(strCsvFileName))
            {
                FileHelper.Delete(strCsvFileName);
            }
            var fi =
                new FileInfo(strCsvFileName);

            if (!DirectoryHelper.Exists(fi.DirectoryName))
            {
                DirectoryHelper.CreateDirectory(fi.DirectoryName);
            }

            //
            // validate titles
            //
            if (events.Count > 0)
            {
                var tsEvent = events.First();
                var properties = ReflectorCache.GetReflector(tsEvent.GetType()).GetReadWritePropertyNames();
                m_strTitles = properties.ToArray();
            }

            //
            // serialize events
            //
            var strCsvFileNameTmp = strCsvFileName + "_tmp";
            using (var sw = new StreamWriter(strCsvFileNameTmp))
            {
                //
                // write titles
                //
                if (m_strTitles != null)
                {
                    sw.Write(m_strTitles[0]);
                    for (var i = 1; i < m_strTitles.Length; i++)
                    {
                        sw.Write("," + m_strTitles[i]);
                    }
                    sw.WriteLine();
                }

                foreach (ITsEvent timeSeriesEvent in events)
                {
                    if (timeSeriesEvent != null)
                    {
                        sw.WriteLine(
                            timeSeriesEvent.ToCsvString());
                    }
                }
            }


            File.Move(
                strCsvFileNameTmp,
                strCsvFileName);

            if (m_blnZipFile)
            {
                var strZipFileName = GetZipFileName(strKey);
                if (FileHelper.Exists(strZipFileName))
                {
                    FileHelper.Delete(strZipFileName);
                }

                //
                // zip file
                //
                var strZipFileNameTmp =
                    strZipFileName + "_tmp";

                ZipHelper.ZipFile(
                    strCsvFileName,
                    strZipFileNameTmp);

                File.Move(
                    strZipFileNameTmp,
                    strZipFileName);

                //
                // tidy up
                //
                FileHelper.Delete(strCsvFileName);
            }

            var strMessage = "Finish writing key:" + strKey + ". Total time = " +
                             (DateTime.Now - startLogTime).TotalSeconds + " seconds.";
            Logger.Log(strMessage);
            SendMessageEvent.OnSendMessage(strMessage);
            return null;
        }

        public void Add(Dictionary<string, List<ITsEvent>> objs)
        {
            List<TaskWrapper> tasks = AddToTask(objs);
            TaskWrapper.WaitAll(tasks.ToArray());
            foreach (TaskWrapper task in tasks)
            {
                task.Dispose();
            }
        }

        public void Add(Dictionary<string, ITsEvent> objs)
        {
            try
            {
                List<TaskWrapper> tasks = AddToTask(objs);
                TaskWrapper.WaitAll(tasks.ToArray());
                foreach (TaskWrapper task in tasks)
                {
                    task.Dispose();
                }
            }
            catch(Exception ex)
            {
                Logger.Log(ex);
            }
        }

        public void Add(string oKey, ITsEvent oValue)
        {
            TaskWrapper task = AddToTask(oKey, oValue);
            task.Wait();
            task.Dispose();
        }

        public void Add(string strKey, List<ITsEvent> events)
        {
            TaskWrapper task = AddToTask(strKey, events);
            task.Wait();
            task.Dispose();
        }

        public virtual void Delete(string strKey)
        {
            string strFileName;
            if (m_blnZipFile)
            {
                strFileName = GetZipFileName(strKey);
            }
            else
            {
                strFileName = GetCsvFileName(strKey);
            }

            if (FileHelper.Exists(strFileName))
            {
                FileHelper.Delete(strFileName);
            }
        }

        public virtual void Delete(List<string> strKeys)
        {
            foreach (string strKey in strKeys)
            {
                Delete(strKey);
            }
        }


        public virtual List<ITsEvent> Get(string strKey)
        {
            var startLogTime = DateTime.Now;

            var strDateDir = GetDateFromKey(strKey);
            var strCsvFileName = GetDataFileName(
                strKey,
                strDateDir);

            var events = new List<ITsEvent>();

            var fileStream = new FileStream(strCsvFileName,
                                            FileMode.Open,
                                            FileAccess.Read,
                                            FileShare.ReadWrite);
            using (var sr = new StreamReader(fileStream))
            {
                //
                // read titles
                //
                var strTitles = sr.ReadLine();
                if (!string.IsNullOrEmpty(strTitles))
                {
                    var currentTitles =
                        strTitles.Split(',');

                    string strLine;
                    while ((strLine = sr.ReadLine()) != null)
                    {
                        var dataProvider =
                            (IDataProvider) ReflectorCache.GetReflector(
                                m_dataProviderProviderType).CreateInstance();
                        
                        var tsEvent =
                                (ITsEvent)ReflectorCache.GetReflector(dataProvider.GetTsEventType()).CreateInstance();
                        HCException.ThrowIfTrue(tsEvent == null,
                            "Null event");

                        TsEventHelper.ParseCsvString(strLine,
                                                     tsEvent,
                                                     currentTitles);
                        events.Add(tsEvent);
                    }
                }
            }

            if (m_blnZipFile)
            {
                FileHelper.Delete(strCsvFileName);
            }

            var strMessage = "Finish reading key:" + strKey + ". Total time = " +
                             (DateTime.Now - startLogTime).TotalSeconds + " seconds.";
            Logger.Log(strMessage);

            return events;
        }

        private string GetDataFileName(
            string strKey,
            string strDateDir)
        {
            var strDir = GetFileDir(strDateDir);

            var strCsvFileName = Path.Combine(
                strDir,
                strKey + ".csv");

            if (m_blnZipFile)
            {
                var strZipFileName = GetZipFileName(strKey);
                HCException.ThrowIfTrue(!FileHelper.Exists(strZipFileName),
                    "File not found. " + strZipFileName);

                ZipHelper.UnZipFile(strZipFileName);
            }
            return strCsvFileName;
        }

        public virtual string GetFileName(DateTime dateTime)
        {
            var strDateDir = DateHelper.ToDateString(dateTime);
            return GetFileDir(strDateDir);
        }

        public string GetFileDir(DateTime dateTime)
        {
            var strDateDir = DateHelper.ToDateString(dateTime);
            return GetFileDir(strDateDir);
        }

        #endregion

        #region Private

        private string GetFileDir(string strDateDir)
        {
            var strDir = m_strPath;
            if (!string.IsNullOrEmpty(strDateDir))
            {
                strDir = Path.Combine(strDir,
                                      strDateDir);
            }
            return strDir;
        }

        public virtual List<ITsEvent> GetAll(DateTime dateTime)
        {
            throw new HCException();
        }

        public virtual List<ITsEvent> GetAll()
        {
            throw new NotImplementedException();
        }

        public virtual List<ITsEvent> GetAll(string strQuery)
        {
            throw new NotImplementedException();
        }

        public virtual Dictionary<string, List<ITsEvent>> GetAllMap(DateTime date)
        {
            throw new NotImplementedException();
        }

        public virtual Dictionary<string, List<ITsEvent>> GetAllMap(string strQuery)
        {
            throw new NotImplementedException();
        }

        public virtual int GetRowCount(DateTime dateTime)
        {
            throw new NotImplementedException();
        }

        public virtual int GetRowCount(string strWhere)
        {
            throw new NotImplementedException();
        }

        public virtual List<string> ContainsKeys(List<string> keys)
        {
            throw new NotImplementedException();
        }

        protected string GetDateFromKey(
            string strKey)
        {

            if(!string.IsNullOrEmpty(DefaultSubPath))
            {
                return DefaultSubPath;
            }
            //
            // get from regex: year, month and day
            //
            return TsDataProviderHelper.GetDateFromKey(strKey);
        }

        private string GetCsvFileName(
            string strKey)
        {
            var strDateDir = GetDateFromKey(strKey);
            var strPath = Path.Combine(
                m_strPath,
                strDateDir);

            return Path.Combine(
                strPath,
                strKey + ".csv");
        }

        private string GetZipFileName(string strKey)
        {
            var strDate = GetDateFromKey(strKey);
            var strPath = m_strPath;

            if (!string.IsNullOrEmpty(strDate))
            {
                strPath = Path.Combine(
                    strPath,
                    strDate);
            }

            strPath = Path.Combine(
                strPath,
                strKey + ".zip");

            var strDirName = new FileInfo(strPath).DirectoryName;
            if (!DirectoryHelper.Exists(strDirName))
            {
                DirectoryHelper.CreateDirectory(strDirName);
            }

            return strPath;
        }

        #endregion

        public virtual int GetCount(DateTime dateTime)
        {
            throw new NotImplementedException();
        }
    }
}
