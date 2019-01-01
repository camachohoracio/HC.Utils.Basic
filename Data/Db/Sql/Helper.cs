#region

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using HC.Core.Io;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public static class Helper
    {
        public static string GetItemList(List<string> itemList)
        {
            if (itemList.Count == 0)
            {
                return "";
            }
            var sb = new StringBuilder();
            sb.Append(itemList[0]);
            for (var i = 1; i < itemList.Count; i++)
            {
                sb.Append(
                    "," + Environment.NewLine +
                    itemList[i]);
            }
            return sb.ToString();
        }

        public static string GetNetworkName(string strServer)
        {
            return strServer.Split(@"\".ToCharArray()[0])[0];
        }

        public static string GetServerName(string strPath)
        {
            var strServerName = "";
            if (strPath[0].Equals(@"\".ToCharArray()[0]) &&
                strPath[1].Equals(@"\".ToCharArray()[0]))
            {
                strPath = strPath.Replace(@"\\", "");
                var tokens = strPath.Split(@"\".ToCharArray()[0]);
                strServerName += tokens[0];
                for (var i = 1; i < tokens.Length; i++)
                {
                    if (tokens[i].Contains("$"))
                    {
                        break;
                    }
                    strServerName += @"\" + tokens[i];
                }
            }
            return strServerName;
        }

        public static string GetDefaultMdfFileName(string strDbName)
        {
            return strDbName + ".mdf";
        }

        public static string GetDefaultLdfFileName(string strDbName)
        {
            return GetDefaultLdfDbName(strDbName) + ".ldf";
        }

        public static string GetDefaultLdfDbName(string strDbName)
        {
            return strDbName + "_log";
        }

        public static void RenameOldDbFile(string strDbName)
        {
            var intCounter = 0;
            var strTempFile = strDbName + "_tmp_" + intCounter;
            while (FileHelper.Exists(strTempFile))
            {
                intCounter++;
                strTempFile = strDbName + "_tmp_" + intCounter;
            }

            if (FileHelper.Exists(strDbName))
            {
                File.Move(strDbName,
                          strTempFile);
            }
        }

        public static bool IsLocalServer(
            string strServerName)
        {
            if (strServerName.ToLower().Equals("(local)") ||
                strServerName.ToLower().Equals(".") ||
                strServerName.ToLower().Equals("localhost"))
            {
                return true;
            }
            return false;
        }

        public static string GetNetworkPath(
            string strServer,
            string strPath)
        {
            var strDriveName = FileHelper.GetDriveName(
                strPath);
            var strDriveLetter = FileHelper.GetDriveLetter(
                strPath);
            return
                @"\\" + strServer + @"\" + strDriveLetter + @"$\" +
                strPath.Replace(strDriveName, "");
        }
    }
}
