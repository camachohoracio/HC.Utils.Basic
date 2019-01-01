#region

using System.Data;
using System.IO;
using HC.Core.Exceptions;
using HC.Core.Io;

#endregion

namespace HC.Utils.Basic.Data.Db
{
    public class ServerConfigurationService : IServerConfigurationService
    {
        private string settingsDirectory = @"\ServerConfigurationService\";
        private string settingsFileName = "dsServerConfig.xml";
        private dsServerConfig _serverConfigData;

        public ServerConfigurationService()
        {
            LoadData();
        }

        #region IServerConfigurationService Members

        public dsServerConfig ServerConfigData
        {
            get { return _serverConfigData; }
            set { _serverConfigData = value; }
        }

        public bool UpdataServerConfigData()
        {
            throw new HCException("Error. Method not defined");
            //using (ServerConfigForm configForm = new ServerConfigForm())
            //{
            //    if (this._serverConfigData != null)
            //    {
            //        configForm.dsServerConfig = this._serverConfigData;
            //    }

            //    if (configForm.ShowDialog() == System.Windows.Forms.DialogResult.OK)
            //    {
            //        this._serverConfigData = configForm.dsServerConfig;

            //        if (CreateFile(Application.UserAppDataPath + settingsDirectory, settingsFileName))
            //        {
            //            _serverConfigData.WriteXml(Application.UserAppDataPath + settingsDirectory + settingsFileName
            //                , XmlWriteMode.IgnoreSchema);
            //        }

            //        return true;
            //    }
            //}

            //return false;
        }

        #endregion

        private void LoadData()
        {
            
            if (FileHelper.Exists(
                FileHelper.GetExecutingAssemblyDir() + settingsDirectory + settingsFileName))
            {
                _serverConfigData = new dsServerConfig();
                _serverConfigData.ReadXml(FileHelper.GetExecutingAssemblyDir() + settingsDirectory + settingsFileName
                                          , XmlReadMode.IgnoreSchema);
                _serverConfigData.AcceptChanges();
            }
        }

        private bool CreateFile(string path, string filename)
        {
            try
            {
                if (!DirectoryHelper.Exists(path))
                {
                    DirectoryHelper.CreateDirectory(path);
                }
                if (!FileHelper.Exists(path + filename))
                {
                    var file = File.Create(path + filename);
                    file.Close();
                }

                return true;
            }
            catch
            {
            }
            return false;
        }
    }
}
