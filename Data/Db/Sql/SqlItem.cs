namespace HC.Utils.Basic.Data.Db.Sql
{
    /// <summary>
    ///   Sql statement data structure
    /// </summary>
    public class SqlItem
    {
        #region Properties

        public string Description { get; set; }
        public string SqlStatement { get; set; }

        #endregion

        /// <summary>
        ///   Default constructor
        /// </summary>
        /// <param name = "strDescription"></param>
        /// <param name = "strSqlStatement"></param>
        public SqlItem(
            string strDescription,
            string strSqlStatement)
        {
            Description = strDescription;
            SqlStatement = strSqlStatement;
        }
    }
}
