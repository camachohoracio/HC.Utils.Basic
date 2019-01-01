#region

using System.Data;

#endregion

namespace HC.Utils.Basic.Data.Db.Parameters
{
    public interface IDbParameter
    {
        #region Properties

        string Name { get; set; }
        int DbTypeId { get; set; }
        int Size { get; set; }
        object Value { get; set; }
        ParameterDirection ParameterDirection { get; set; }

        #endregion

        #region Interface Methods

        DbInputParameter GetValue(
            string strName,
            object value);

        #endregion
    }
}
