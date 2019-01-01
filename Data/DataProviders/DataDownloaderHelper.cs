#region

using System.Collections.Concurrent;
using HC.Core.Threading;

#endregion

namespace HC.Utils.Basic.Data.DataProviders
{
    public static class DataDownloaderHelper
    {
        #region Members

        private static ConcurrentBag<ThreadWorker> m_theads;
        private static ConcurrentDictionary<string, int> m_timers;
        private static ConcurrentDictionary<string, string> m_assemblyLookup;

        #endregion

        private static void LoadAssemblyNames()
        {
            m_assemblyLookup = new ConcurrentDictionary<string, string>();
            m_timers = new ConcurrentDictionary<string, int>();
            var assemblyList = Basic.Config.GetAssemblyList();
            foreach (string strAssemblyDescr in assemblyList)
            {
                var tokens = strAssemblyDescr.Split(',');
                var strAssemblyId = tokens[0];
                m_assemblyLookup[strAssemblyId] = tokens[1];
                m_timers[strAssemblyId] = int.Parse(tokens[2]);
            }
        }

        //public static void StartDataDownloader(
        //    string strAppName,
        //    string strExeFileName)
        //{
        //    try
        //    {
        //        LoadAssemblyNames();
        //        m_theads = new ConcurrentBag<ThreadWorker>();
        //        StartAppDomainWorker(
        //            strAppName, 
        //            strExeFileName);
        //    }
        //    catch (Exception e)
        //    {
        //        Logger.Log("A critical error occurred: " + e);
        //        return;
        //    }
        //}

        //public static void StartDataDownloaders()
        //{
        //    try
        //    {
        //        LoadAssemblyNames();
        //        m_theads = new ConcurrentBag<ThreadWorker>();
        //        foreach (KeyValuePair<string, string> keyValuePair in m_assemblyLookup)
        //        {
        //            StartAppDomainWorker(keyValuePair.Key, keyValuePair.Value);
        //        }

        //        //YahooRssNewsDownloader.LoadData();
        //        //YahooQuotesDownloader.LoadData();
        //        //YahooStaticTsDataDownloader.LoadData();
        //        //TwitterDownloader.LoadData();
        //    }
        //    catch (Exception e)
        //    {
        //        Logger.Log("A critical error occurred: " + e);
        //        return;
        //    }
        //}

        //public static void StartAppDomainWorker(
        //    string strAppName,
        //    string strExeFileName)
        //{
        //    var threadWorker = new ThreadWorker();
        //    threadWorker.OnExecute += () => RunDomain(strExeFileName, strAppName);
        //    threadWorker.WaitForExit = false;
        //    threadWorker.Work();
        //    m_theads.Add(threadWorker);
        //}

        //private static void RunDomain(string strExeFileName, string strAppName)
        //{
        //    try
        //    {
        //        Logger.Log("Starting domain: " + strAppName + "...");
        //        var domaininfo = new AppDomainSetup();
        //        domaininfo.ConfigurationFile =
        //            //Path.Combine(FileHelper.GetExecutingAssemblyDir(),
        //            strExeFileName + ".config"; //);
        //        domaininfo.ApplicationName = strAppName;
        //        var adevidence = AppDomain.CurrentDomain.Evidence;
        //        var appDomain = AppDomain.CreateDomain(strAppName, adevidence, domaininfo);
        //        appDomain.ExecuteAssembly(strExeFileName);
        //        appDomain.ProcessExit += AppDomainDomainUnload;
        //        Logger.Log("Started domain: " + strAppName);
        //    }
        //    catch (Exception ex)
        //    {
        //        Logger.Log(ex);
        //    }
        //}

        //private static void AppDomainDomainUnload(object sender, EventArgs e)
        //{
        //    //
        //    // domain finishes its execution. 
        //    // Restart after x period of time
        //    //
        //    var appDomain = (AppDomain) sender;
        //    var strAssenblyName = appDomain.FriendlyName;
        //    var intTimer = m_timers[strAssenblyName];
        //    var strAssemblyFile = m_assemblyLookup[strAssenblyName];
        //    RunDomain(strAssemblyFile, strAssenblyName);
        //    Thread.Sleep(intTimer*1000);
        //}
    }
}
