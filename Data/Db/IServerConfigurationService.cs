#region

#endregion

namespace HC.Utils.Basic.Data.Db
{
    public interface IServerConfigurationService
    {
        dsServerConfig ServerConfigData { get; set; }

        bool UpdataServerConfigData();
    }
}
