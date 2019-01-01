using System.Collections.Generic;
using HC.Analytics.TimeSeries;

namespace HC.Utils.Basic.Data.Tests
{
    public class FooTsEvent : ATsEvent
    {
        public string Symbol { get; set; }
        public List<string> ListStr { get; set; }
        public double DblValue { get; set; }
        public int ReadOnly { get; private set; }

        public FooTsEvent()
        {
            ReadOnly = 654;
        }
    }
}
