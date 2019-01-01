#region

using System.Data.SqlClient;
using System.Threading;
using HC.Core.Io;

#endregion

namespace HC.Utils.Basic.Data.Db.Sql
{
    public class DbManagerService
    {
        #region Events

        #region Delegates

        public delegate void FinishProcessEventHandler();

        public delegate void SendMessageEventHandler(
            string strMessage,
            int intProgress);

        #endregion

        public event SendMessageEventHandler SendMessage;

        public event FinishProcessEventHandler finishProcessEventHandler;

        #endregion

        #region Members

        private SqlConnectionStringBuilder m_connectionStringBuilder;
        private string m_strNewDbName;
        private string m_strPath;

        #endregion

        #region Public

        public void RenameDb(
            SqlConnectionStringBuilder connectionStringBuilder,
            string strNewDbName)
        {
            m_strNewDbName = strNewDbName;
            m_connectionStringBuilder = connectionStringBuilder;

            ThreadStart job =
                RunRename;
            var thread = new Thread(job);
            thread.Start();
        }

        public void BackUpDb(
            SqlConnectionStringBuilder connectionStringBuilder,
            string strPath)
        {
            m_strPath = strPath;
            m_connectionStringBuilder = connectionStringBuilder;

            ThreadStart job =
                RunBackUp;
            var thread = new Thread(job);
            thread.Start();
        }

        public void DoMove(
            SqlConnectionStringBuilder connectionStringBuilder,
            string strPath)
        {
            m_strPath = strPath;
            m_connectionStringBuilder = connectionStringBuilder;

            ThreadStart job =
                RunMove;
            var thread = new Thread(job);
            thread.Start();
        }


        public void ShrinkDb(SqlConnectionStringBuilder connectionStringBuilder)
        {
            m_connectionStringBuilder = connectionStringBuilder;
            ThreadStart job =
                RunShrink;
            var thread = new Thread(job);
            thread.Start();
        }

        public void RestoreDb(
            SqlConnectionStringBuilder connectionStringBuilder,
            string strDbName,
            string strSourceMdfFileName,
            string strSourceLdfFileName,
            string strDesinationMdfFileName,
            string strDesinationLdfFileName)
        {
            if (SqlCommandHelper.CheckDbExists(
                strDbName,
                connectionStringBuilder))
            {
                SqlCommandHelper.DetatchDb(
                    strDbName,
                    connectionStringBuilder);
            }

            Helper.RenameOldDbFile(strDesinationMdfFileName);
            Helper.RenameOldDbFile(strDesinationLdfFileName);

            var fileTransfer = new FileTransferHelper();
            fileTransfer.progressBarEventHandler +=
                InvokeSendMessage;


            fileTransfer.CopyFile(
                strSourceMdfFileName,
                strDesinationMdfFileName);

            if (!strSourceLdfFileName.Equals(string.Empty))
            {
                fileTransfer.CopyFile(
                    strSourceLdfFileName,
                    strDesinationLdfFileName);
            }

            SqlCommandHelper.AttachDB(
                strDbName,
                strDesinationMdfFileName,
                strDesinationLdfFileName,
                strDesinationMdfFileName,
                strDesinationLdfFileName,
                connectionStringBuilder);
        }

        #endregion

        #region Private

        private void RunRename()
        {
            SqlFileHelper.SendMessage +=
                InvokeSendMessage;

            SqlFileHelper.RenameDbPhysicalFiles(
                m_connectionStringBuilder.InitialCatalog,
                m_strNewDbName,
                m_connectionStringBuilder);

            SqlFileHelper.SendMessage -=
                InvokeSendMessage;

            InvokeFinishProcess();
        }

        private void RunShrink()
        {
            SqlFileHelper.SendMessage +=
                InvokeSendMessage;

            SqlCommandHelper.ShinkDatabase(
                m_connectionStringBuilder.InitialCatalog,
                m_connectionStringBuilder);

            SqlFileHelper.SendMessage -=
                InvokeSendMessage;

            InvokeFinishProcess();
        }

        private void RunBackUp()
        {
            SqlFileHelper.SendMessage +=
                InvokeSendMessage;

            SqlFileHelper.BackUpExistingDb(
                m_connectionStringBuilder.InitialCatalog,
                m_strPath,
                m_connectionStringBuilder);

            SqlFileHelper.SendMessage -=
                InvokeSendMessage;

            InvokeFinishProcess();
        }

        private void RunMove()
        {
            SqlFileHelper.SendMessage +=
                InvokeSendMessage;

            SqlFileHelper.MoveDatabaseLocation(
                m_connectionStringBuilder.InitialCatalog,
                m_connectionStringBuilder.InitialCatalog,
                m_strPath,
                m_connectionStringBuilder);

            SqlFileHelper.SendMessage -=
                InvokeSendMessage;

            InvokeFinishProcess();
        }

        private void InvokeFinishProcess()
        {
            if (finishProcessEventHandler != null)
            {
                if (finishProcessEventHandler.GetInvocationList().Length > 0)
                {
                    finishProcessEventHandler.Invoke();
                }
            }
        }

        private void InvokeSendMessage(string strMessage, int intProgress)
        {
            if (SendMessage != null)
            {
                if (SendMessage.GetInvocationList().Length > 0)
                {
                    SendMessage.Invoke(strMessage, intProgress);
                }
            }
        }

        #endregion
    }
}
