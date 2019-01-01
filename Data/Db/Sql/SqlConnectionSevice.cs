#region

using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using HC.Core.Events;
using HC.Core.Exceptions;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public static class SqlConnectionSevice
    {
        #region Members

        public static List<string> m_bannedConnectionStrings = new List<string>();

        public static Dictionary<string, string> m_connectionStrings =
            new Dictionary<string, string>();

        #endregion

        public static SqlConnectionStringBuilder ValidateConnection(
            string strServerName)
        {
            return ValidateConnection(
                strServerName,
                "master");
        }

        public static SqlConnectionStringBuilder ValidateConnection(
            string strServerName,
            string strDbName)
        {
            var sqlConnectionStringBuilder =
                SqlConnectionStringHelper.GetTrustedConnectionStringBuilder(
                    strServerName,
                    strDbName);
            var strKey = Db.Helper.GetConnectionName(
                sqlConnectionStringBuilder.ConnectionString);

            if (m_connectionStrings.ContainsKey(strKey))
            {
                //
                // retrieve existing connection
                //
                sqlConnectionStringBuilder =
                    new SqlConnectionStringBuilder(
                        m_connectionStrings[strKey]);
            }
            else
            {
                //
                // try with trusted connection
                //
                sqlConnectionStringBuilder =
                    SqlConnectionStringHelper.GetTrustedConnectionStringBuilder(
                        strServerName,
                        strDbName);

                AddNewConnectionString(
                    sqlConnectionStringBuilder);
            }

            //
            // Save connection in case it is valid
            //
            sqlConnectionStringBuilder = ValidateConnection(
                sqlConnectionStringBuilder);
            AddNewConnectionString(
                sqlConnectionStringBuilder);

            return sqlConnectionStringBuilder;
        }

        private static void AddNewConnectionString(
            SqlConnectionStringBuilder sqlConnectionStringBuilder)
        {
            var strKey = Db.Helper.GetConnectionName(
                sqlConnectionStringBuilder.ConnectionString);
            //
            // Save connection in case it is valid
            //
            if (SqlCommandHelper.CheckDbExists(
                sqlConnectionStringBuilder.InitialCatalog,
                sqlConnectionStringBuilder))
            {
                if (!m_connectionStrings.ContainsKey(strKey))
                {
                    m_connectionStrings.Add(strKey,
                                            sqlConnectionStringBuilder.ConnectionString);
                }
            }
        }


        /// <summary>
        ///   Check if connection is valid. 
        ///   Otherwise ask user to correct connection settings
        /// </summary>
        /// <param name = "connectionString"></param>
        private static SqlConnectionStringBuilder ValidateConnection(
            SqlConnectionStringBuilder connectionString)
        {
            connectionString = ValidateConnection(connectionString,
                                                  true);
            if (!SqlCommandHelper.CheckDbExists(
                connectionString.InitialCatalog,
                connectionString))
            {
                m_bannedConnectionStrings.Add(connectionString.ConnectionString);
                throw new HCException("Error. DB connection is invalid");
            }
            return connectionString;
        }

        public static SqlConnectionStringBuilder ValidateConnection(
            SqlConnectionStringBuilder connectionString,
            bool blnValidationFlag)
        {
            try
            {
                SendMessageEvent.OnSendMessage(
                    null,
                    "Connecting to server: " + connectionString.DataSource + "...");
                if (!SqlCommandHelper.CheckDbExists(
                    connectionString.InitialCatalog,
                    connectionString))
                {
                    throw new HCException("Error. DB connection is invalid");
                }

                return connectionString;
            }
            catch (Exception e)
            {
                if (blnValidationFlag)
                {
                    if (!m_bannedConnectionStrings.Contains(connectionString.ConnectionString))
                    {
                        SendMessageEvent.OnSendMessage(
                            null,
                            "Failure while connecting to server: " +
                            connectionString.DataSource +
                            ". Trying to reconnect...");
                        return LaunchDbValidatorForm(connectionString);
                    }
                    else
                    {
                        throw;
                    }
                }
                else
                {
                    throw;
                }
            }
        }

        public static SqlConnectionStringBuilder SetDbConnection()
        {
            var sqlConnectionStringBuilder =
                LaunchDbValidatorForm();
            AddNewConnectionString(
                sqlConnectionStringBuilder);
            return sqlConnectionStringBuilder;
        }

        public static SqlConnectionStringBuilder LaunchDbValidatorForm()
        {
            return LaunchDbValidatorForm(null);
        }

        public static SqlConnectionStringBuilder LaunchDbValidatorForm(
            SqlConnectionStringBuilder connectionString)
        {
            return connectionString;
            ////
            //// the connection string is invalid
            //// load same connection with password settings
            ////
            //var frmExecuteCommandOnDb =
            //    new FrmExecuteCommandOnDb();
            ////
            //// set form's initial conditions
            ////
            //frmExecuteCommandOnDb.Title =
            //    "Set database connection...";
            //frmExecuteCommandOnDb.UcExecuteCommandOnDb_.ButtonLabel =
            //    "GetController";

            ////
            //// Set server and database settings
            ////
            //if (connectionString != null)
            //{
            //    frmExecuteCommandOnDb.UcExecuteCommandOnDb_.DatabaseDetails.SetServer(
            //        connectionString.DataSource);
            //    frmExecuteCommandOnDb.UcExecuteCommandOnDb_.DatabaseDetails.SetDb(
            //        connectionString.InitialCatalog);
            //    // lock server and database combo boxes
            //    frmExecuteCommandOnDb.UcExecuteCommandOnDb_.
            //        DatabaseDetails.LockServerDb();
            //    // register execute command event
            //    frmExecuteCommandOnDb.UcExecuteCommandOnDb_.ExecuteCommand +=
            //        InvokeDbValidation;
            //}
            //else
            //{
            //    frmExecuteCommandOnDb.UcExecuteCommandOnDb_.ExecuteCommand +=
            //        LoopDbValidation;
            //}

            //frmExecuteCommandOnDb.TopMost = true;
            //frmExecuteCommandOnDb.ShowDialog();
            //frmExecuteCommandOnDb.Activate();

            //return frmExecuteCommandOnDb.UcExecuteCommandOnDb_.
            //    DatabaseDetails.ConnectionStringBuilder;
        }

        private static void LoopDbValidation(
            object sender)
        {
            //var ucExecuteCommandOnDb = (UcExecuteCommandOnDb) sender;
            //var connectionString =
            //    ucExecuteCommandOnDb.DatabaseDetails.ConnectionStringBuilder;

            ////
            //// Chevk db connection
            ////
            //if (!SqlCommandHelper.CheckDbExists(
            //    connectionString.InitialCatalog,
            //    connectionString))
            //{
            //    Console.WriteLine("DB connection is invalid. Please try again.");
            //}
        }


        private static void InvokeDbValidation(
            object sender)
        {
            //var ucExecuteCommandOnDb =
            //    (UcExecuteCommandOnDb) sender;

            //((FrmExecuteCommandOnDb) ucExecuteCommandOnDb.Parent).Close();
            ////
            //// execute once more
            ////
            //ValidateConnection(
            //    ucExecuteCommandOnDb.
            //        DatabaseDetails.ConnectionStringBuilder,
            //    false);
        }
    }
}
