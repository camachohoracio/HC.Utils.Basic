#region

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using HC.Core.Exceptions;
using HC.Core.Io;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public class DbCheckService
    {
        /// <summary>
        ///   Check if the dataset contains any db files
        /// </summary>
        /// <param name = "dsDataTransfer"></param>
        /// <param name = "checkedItemsList"></param>
        /// <returns></returns>
        public bool ContainsDbs(
            DsDataTransfer dsDataTransfer,
            List<string> checkedItemsList)
        {
            for (int i = 0; i < dsDataTransfer.data_transfer.Count; i++)
            {
                DsDataTransfer.data_transferRow row =
                    dsDataTransfer.data_transfer[i];
                if (checkedItemsList.Contains(row.transfer_description))
                {
                    //
                    // Get files
                    //
                    var fileDetailsRow =
                        row.Getdata_transfer_detailsRows();
                    if (fileDetailsRow.Length > 0)
                    {
                        // iterate each file
                        for (var j = 0; j < fileDetailsRow.Length; j++)
                        {
                            var strFileName = fileDetailsRow[j].file_name;
                            var fi = new FileInfo(strFileName);
                            if (FileHelper.CheckIsDatabaseFile(
                                strFileName))
                            {
                                return true;
                            }
                        }
                    }
                    else
                    {
                        var strFilePaths = Directory.GetFiles(
                            row.source_path,
                            "*.*",
                            SearchOption.AllDirectories);
                        for (var j = 0; j < strFilePaths.Length; j++)
                        {
                            if (FileHelper.CheckIsDatabaseFile(
                                strFilePaths[j]))
                            {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }


        public bool CheckDbs(
            DsDataTransfer dsDataTransfer,
            string strRootPath,
            List<string> checkedItemsList)
        {
            //
            // Check root path
            //
            var strServerName = Helper.GetServerName(
                strRootPath);
            var connectionString =
                SqlConnectionStringHelper.GetTrustedConnectionStringBuilder(
                    strServerName);
            var diRootPath = new DirectoryInfo(strRootPath);
            if (!DirectoryHelper.Exists(strRootPath))
            {
                DirectoryHelper.CreateDirectory(strRootPath);
            }

            for (int i = 0; i < 
                dsDataTransfer.data_transfer.Count; i++)
            {
                DsDataTransfer.data_transferRow row =
                    dsDataTransfer.data_transfer[i];
                if (checkedItemsList.Contains(row.transfer_description))
                {
                    var strSourcePath = row.source_path;
                    var strDestinationPath = diRootPath.FullName + 
                                             @"\" + row.destination_path;
                    var diSourcePath = new DirectoryInfo(strSourcePath);
                    var diDestinationPath = new DirectoryInfo(strDestinationPath);
                    //
                    // Get files
                    //
                    var fileDetailsRow =
                        row.Getdata_transfer_detailsRows();
                    if (fileDetailsRow.Length > 0)
                    {
                        // iterate each file
                        for (var j = 0; j < fileDetailsRow.Length; j++)
                        {
                            var strFileName = fileDetailsRow[j].file_name;
                            var fi = new FileInfo(diSourcePath.FullName + @"\" +
                                                  strFileName);
                            var strDbName = fi.Name.Replace(fi.Extension, "");
                            var strDestinationFile =
                                diDestinationPath.FullName + @"\" +
                                strFileName;

                            if (fi.Extension.ToLower().Equals(".mdf") &&
                                SqlCommandHelper.CheckDbExists(
                                    strDbName,
                                    connectionString))
                            {
                                SqlFileHelper.PutAsideDatabase(
                                    strDbName,
                                    connectionString);
                            }
                            else if (FileHelper.Exists(strDestinationFile))
                            {
                                try
                                {
                                    TestLockFile(strDestinationFile);
                                    TestLogFile(strDestinationFile);
                                }
                                catch (Exception e2)
                                {
                                    throw new HCException("Error. Db " +
                                                                 strDbName +
                                                                 " could not be replaced. " +
                                                                 e2.Message);
                                }
                            }
                        }
                    }
                }
            }
            return true;
        }

        private static void TestLockFile(string strDestinationFile)
        {
            if (FileHelper.Exists(strDestinationFile + "_tmp"))
            {
                FileHelper.Delete(strDestinationFile + "_tmp");
            }
            //rename file
            File.Move(strDestinationFile,
                      strDestinationFile + "_tmp");
            File.Move(strDestinationFile + "_tmp",
                      strDestinationFile);
        }

        private static void TestLogFile(string strDestinationFile)
        {
            var fi = new FileInfo(strDestinationFile);
            var strDbName = fi.Name.Replace(fi.Extension, "");
            var strLogFile = fi.DirectoryName + @"\" +
                             Helper.GetDefaultLdfFileName(
                                 strDbName);
            if (FileHelper.Exists(strLogFile))
            {
                if (FileHelper.Exists(strLogFile + "_tmp"))
                {
                    FileHelper.Delete(strLogFile + "_tmp");
                }
                File.Move(strLogFile,
                          strLogFile + "_tmp");
            }
        }


        public static bool CheckNewDb(
            SqlConnectionStringBuilder connectionStringBuilder,
            string strDbName,
            string strMdfFileName,
            string strLdfFileName)
        {
            if (!CheckDb(strDbName,
                         connectionStringBuilder))
            {
                return false;
            }

            //
            //check if the file belongs to an existing database name
            //
            var dbList = SqlCommandHelper.GetDbList(
                connectionStringBuilder);

            foreach (string strDb in dbList)
            {
                if (SqlFileHelper.GetMdfFullPath(
                    strDb,
                    connectionStringBuilder).Equals(
                        strMdfFileName))
                {
                    if (!CheckDb(strDb,
                                 connectionStringBuilder))
                    {
                        return false;
                    }
                }
            }

            //
            // Check if DB files exists in the designated path
            //
            if (!FileHelper.CheckFileExists(strMdfFileName))
            {
                return false;
            }
            if (!FileHelper.CheckFileExists(strLdfFileName))
            {
                return false;
            }
            return true;
        }


        public static bool CheckDb(
            string strDbName,
            SqlConnectionStringBuilder connectionStringBuilder)
        {
            //
            // Check if DB exists in server
            //
            if (SqlCommandHelper.CheckDbExists(
                strDbName,
                connectionStringBuilder))
            {
                //var strMessage = "Database: " + strDbName +
                //                 " already exists. Do you wish to delete existing DB?";
                //var buttons = MessageBoxButtons.YesNo;
                //DialogResult result;
                //result = MessageBoxWrapper.Question(strMessage, buttons);
                //if (result == DialogResult.No)
                //{
                //    MessageBoxWrapper.Information("The process has been cancelled.");
                //    return false;
                //}
                //else
                {
                    SqlFileHelper.PutAsideDatabase(
                        strDbName,
                        connectionStringBuilder);
                }
            }
            return true;
        }
    }
}
