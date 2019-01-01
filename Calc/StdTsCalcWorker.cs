#region

using System;
using System.Collections.Generic;
using HC.Core.Cache;
using HC.Core.Distributed.Worker;
using HC.Core.DynamicCompilation;

#endregion

namespace HC.Utils.Basic.Calc
{
    public delegate void WorkDelegate(object objSender);

    public class StdCalcWorker : ITsCalcWorker
    {
        #region Members

        private readonly WorkDelegate m_workDelegate;

        #endregion

        #region Properties

        public object ObjSender { get; private set; }
        public string Resource { get; set; }
        public ASelfDescribingClass Params { get; set; }

        public List<ITsEvent> TsEvents
        {
            get { return null; }
            set { throw new NotImplementedException(); }
        }

        public CacheDictionary<string, List<ITsEvent>> Cache
        {
            get { return null; }
            set { throw new NotImplementedException(); }
        }

        public bool DoCache
        {
            get { return false; }
            set { throw new NotImplementedException(); }
        }

        #endregion

        #region Constructors

        public StdCalcWorker() {}

        public StdCalcWorker(
            WorkDelegate workDelegate,
            object objSender)
        {
            m_workDelegate = workDelegate;
            ObjSender = objSender;
        }

        #endregion

        #region Public

        public void Work()
        {
            m_workDelegate(ObjSender);
        }

        public string GetResourceName()
        {
            return string.Empty;
        }

        #endregion

        public void Dispose()
        {
            ObjSender = null;
            Resource = null;
            if(Params !=null)
            {
                Params.Dispose();
                Params = null;
            }
        }
    }
}
