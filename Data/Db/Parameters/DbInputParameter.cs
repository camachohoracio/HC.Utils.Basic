#region

using System.Data;

#endregion

namespace HC.Utils.Basic.Data.Db.Parameters
{
    public class DbInputParameter : AbstractDbParameter
    {
        #region Constructors

        public DbInputParameter(
            string strName,
            object value,
            DbProviderType dbProviderType,
            int intDbTypeId)
            : base(
                strName,
                value,
                ParameterDirection.Input,
                dbProviderType,
                intDbTypeId)
        {
        }

        #endregion

        #region Public

        public override DbInputParameter GetValue(
            string strName,
            object value)
        {
            return new DbInputParameter(
                strName,
                value,
                DbProviderType_,
                Helper.GetDbTypeId(value.GetType()));
        }

        #endregion
    }
}
