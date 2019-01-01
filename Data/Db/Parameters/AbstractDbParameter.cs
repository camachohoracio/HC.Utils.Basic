#region

using System.Data;

#endregion

namespace HC.Utils.Basic.Data.Db.Parameters
{
    public abstract class AbstractDbParameter : IDbParameter
    {
        #region Properties

        public DbProviderType DbProviderType_ { get; set; }
        public string Name { get; set; }
        public int DbTypeId { get; set; }
        public int Size { get; set; }
        public object Value { get; set; }
        public ParameterDirection ParameterDirection { get; set; }

        #endregion

        #region Constructors

        public AbstractDbParameter(
            string strName,
            object value,
            ParameterDirection parameterDirection,
            DbProviderType dbProviderType,
            int intDbTypeId)
        {
            Value = value;
            Name = strName;
            DbTypeId = intDbTypeId;
            DbProviderType_ = dbProviderType;
            ParameterDirection = parameterDirection;
        }

        #endregion

        #region Public

        #endregion

        #region AbstractMethods

        public abstract DbInputParameter GetValue(
            string strName,
            object value);

        #endregion
    }
}
