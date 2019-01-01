#region

using System.Data.SqlClient;
using System.IO;
using HC.Core.Exceptions;
using HC.Core.Io;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public static class SqlFileHelper
    {
        #region Events

        #region Delegates

        public delegate void SendMessageEventHandler(
            string strMessage,
            int intProgress);

        #endregion

        public static event SendMessageEventHandler SendMessage;

        #endregion

        #region Public

        public static string GetDbPhysicalFullName(
            string strDbName,
            SqlConnectionStringBuilder connectionString,
            bool blnIsLogFile)
        {
            var strFileExtension = "";
            if (blnIsLogFile)
            {
                strFileExtension = ".ldf";
            }
            else
            {
                strFileExtension = ".mdf";
            }

            var strServerName = connectionString.DataSource;
            var strQuery = SqlStatementHelper.GetDbPhysicalFilesNamesStatement(strDbName);
            using (var reader = SqlCommandHelper.GetReader(
                strQuery,
                connectionString))
            {
                reader.Read();
                var strPath1 = reader.GetString(0);
                if (FileHelper.GetExtension(strPath1).ToLower().Equals(strFileExtension))
                {
                    return strPath1;
                }
                reader.Read();
                var strPath2 = reader.GetString(0);
                if (FileHelper.GetExtension(strPath2).ToLower().Equals(strFileExtension))
                {
                    return strPath2;
                }
                reader.Close();
            }
            throw new HCException("Error. Db file not found");
        }

        public static void RenameDbPhysicalFiles(
            string strOldDbName,
            string strNewDbName,
            SqlConnectionStringBuilder connectionString)
        {
            var strDbPath = GetMdfFullPath(strOldDbName,
                                           connectionString);

            MoveDatabaseLocation(
                strOldDbName,
                strNewDbName,
                strDbPath,
                connectionString);
        }

        public static void MoveDatabaseLocation(
            string strOldDbName,
            string strNewDbName,
            string strDestinationPath,
            SqlConnectionStringBuilder connectionString)
        {
            string strOldNetworkMdfFullFileName;
            string strOldNetworkLdfFullFileName;

            string strOldMdfFullFileName;
            string strOldLdfFullFileName;

            MoveDatabaseLocation0(
                strOldDbName,
                strNewDbName,
                connectionString,
                out strOldNetworkMdfFullFileName,
                out strOldNetworkLdfFullFileName,
                out strOldMdfFullFileName,
                out strOldLdfFullFileName);

            //
            // detach database
            //
            //SqlCommandHelper.DetatchDb(
            //    strNewDbName,
            //    connectionString);

            //
            // rename physical files
            //
            var fileTransfer = new FileTransferHelper();
            fileTransfer.progressBarEventHandler +=
                InvokeProgressBar;

            var strNewMdfFullFileName = strDestinationPath + @"\" +
                                        Helper.GetDefaultMdfFileName(
                                            strNewDbName);
            var strNewLdfFullFileName = strDestinationPath + @"\" +
                                        Helper.GetDefaultLdfFileName(
                                            strNewDbName);

            SqlCommandHelper.DetatchDb(
                strNewDbName,
                connectionString);

            fileTransfer.MoveFile(
                strOldNetworkMdfFullFileName,
                strNewMdfFullFileName);

            fileTransfer.MoveFile(
                strOldNetworkLdfFullFileName,
                strNewLdfFullFileName);

            // attach new physical files
            SqlCommandHelper.AttachDB(
                strNewDbName,
                strDestinationPath,
                connectionString);
        }

        private static void MoveDatabaseLocation0(
            string strOldDbName,
            string strNewDbName,
            SqlConnectionStringBuilder connectionString,
            out string strOldNetworkMdfFullFileName,
            out string strOldNetworkLdfFullFileName,
            out string strOldMdfFullFileName,
            out string strOldLdfFullFileName)
        {
            var strServerName = connectionString.DataSource;

            if (!SqlCommandHelper.CheckDbExists(strOldDbName, connectionString))
            {
                throw new HCException("Error. Db not found");
            }
            strOldNetworkMdfFullFileName = "";
            strOldNetworkLdfFullFileName = "";
            strOldMdfFullFileName = "";
            strOldLdfFullFileName = "";

            strOldNetworkMdfFullFileName = GetMdfFullFileName(
                strOldDbName,
                connectionString);

            strOldNetworkLdfFullFileName = GetLdfFullFileName(
                strOldDbName,
                connectionString);

            //
            // get the old and new file names
            //
            var strOldMdfFileName = new FileInfo(strOldNetworkMdfFullFileName).Name;
            var strOldLdfFileName = new FileInfo(strOldNetworkLdfFullFileName).Name;

            if (!strOldDbName.Equals(strNewDbName))
            {
                //
                // Change db logical name
                //
                SqlCommandHelper.RenameLogicalDbName(
                    strOldDbName,
                    strNewDbName,
                    connectionString);

                //
                // Change db name
                //
                SqlCommandHelper.RenameDbName(
                    strOldDbName,
                    strNewDbName,
                    connectionString);
            }

            strOldMdfFullFileName = strOldNetworkMdfFullFileName;
            strOldLdfFullFileName = strOldNetworkLdfFullFileName;

            //
            // get relative file paths
            //
            if (!strServerName.ToLower().Equals(".") &&
                !strServerName.ToLower().Equals("(local)") &&
                !strServerName.ToLower().Equals("localhost"))
            {
                strOldNetworkMdfFullFileName =
                    strOldNetworkMdfFullFileName.Replace(
                        FileHelper.GetDriveName(strOldNetworkMdfFullFileName),
                        @"\\" + strServerName + @"\" +
                        FileHelper.GetDriveLetter(strOldNetworkMdfFullFileName) + @"$\");

                strOldNetworkLdfFullFileName =
                    strOldNetworkLdfFullFileName.Replace(
                        FileHelper.GetDriveName(strOldNetworkLdfFullFileName),
                        @"\\" + strServerName + @"\" +
                        FileHelper.GetDriveLetter(strOldNetworkLdfFullFileName) + @"$\");
            }
        }


        public static string GetMdfFullPath(
            string strDbName,
            SqlConnectionStringBuilder connectionString)
        {
            var strDbFileName = GetMdfFullFileName(
                strDbName,
                connectionString);
            return new FileInfo(strDbFileName).DirectoryName;
        }

        public static string GetLdfFullPath(
            string strDbName,
            SqlConnectionStringBuilder connectionString)
        {
            var strDbFileName = GetLdfFullFileName(
                strDbName,
                connectionString);
            return new FileInfo(strDbFileName).DirectoryName;
        }


        ///// <summary>
        ///// Get a list of MDF file included in the current server
        ///// </summary>
        ///// <param name="connectionString"></param>
        ///// <returns></returns>
        //public static List<string> GetMdfServerList(
        //    SqlConnectionStringBuilder connectionString)
        //{ 
        //    SqlCommandHelper.db
        //}

        public static string GetMdfFullFileName(
            string strDbName,
            SqlConnectionStringBuilder connectionString)
        {
            return GetDbPhysicalFullName(
                strDbName,
                connectionString,
                false);
        }

        public static string GetLdfFullFileName(
            string strDbName,
            SqlConnectionStringBuilder connectionString)
        {
            return GetDbPhysicalFullName(
                strDbName,
                connectionString,
                true);
        }

        public static void BackUpExistingDb(
            string strDbName,
            string strDestinationPath,
            SqlConnectionStringBuilder connectionString)
        {
            string strOldNetworkMdfFullFileName;
            string strOldNetworkLdfFullFileName;

            string strOldMdfFullFileName;
            string strOldLdfFullFileName;

            MoveDatabaseLocation0(
                strDbName,
                strDbName,
                connectionString,
                out strOldNetworkMdfFullFileName,
                out strOldNetworkLdfFullFileName,
                out strOldMdfFullFileName,
                out strOldLdfFullFileName);

            var fileTransfer = new FileTransferHelper();
            fileTransfer.progressBarEventHandler +=
                InvokeProgressBar;

            SqlCommandHelper.DetatchDb(
                strDbName,
                connectionString);

            fileTransfer.CopyFile(
                strOldNetworkMdfFullFileName,
                strDestinationPath,
                false);

            fileTransfer.CopyFile(
                strOldNetworkLdfFullFileName,
                strDestinationPath,
                false);


            // attach new physical files
            SqlCommandHelper.AttachDB(
                strDbName,
                new FileInfo(strOldMdfFullFileName).DirectoryName,
                connectionString);
        }

        /// <summary>
        ///   detach datbase and rename its physical name in order to create a new database
        /// </summary>
        public static void PutAsideDatabase(
            string strDbName,
            SqlConnectionStringBuilder connectionString)
        {
            if (!SqlCommandHelper.CheckDbExists(strDbName,
                                                connectionString))
            {
                throw new HCException("Error. Database not found.");
            }

            var strOldMdfFileName =
                GetMdfFullFileName(
                    strDbName,
                    connectionString);

            var strOldLdfFileName =
                GetLdfFullFileName(
                    strDbName,
                    connectionString);

            SqlCommandHelper.DetatchDb(
                strDbName,
                connectionString);

            var strNewMdfFileName =
                strOldMdfFileName + "_tmp";

            var strNewLdfFileName =
                strOldLdfFileName + "_tmp";

            if (FileHelper.Exists(strNewMdfFileName))
            {
                FileHelper.Delete(strNewMdfFileName);
            }
            if (FileHelper.Exists(strNewLdfFileName))
            {
                FileHelper.Delete(strNewLdfFileName);
            }
            // move files
            File.Move(strOldMdfFileName, strNewMdfFileName);
            File.Move(strOldLdfFileName, strNewLdfFileName);
        }

        #endregion

        #region Private

        private static void InvokeProgressBar(
            string message,
            int percentage)
        {
            if (SendMessage != null)
            {
                if (SendMessage.GetInvocationList().Length > 0)
                {
                    SendMessage.Invoke(message,
                                       percentage);
                }
            }
        }

        #endregion
    }
}
