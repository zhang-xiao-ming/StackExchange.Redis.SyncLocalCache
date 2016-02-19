using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis.SyncLocalCache
{
    [Serializable]
    public class SyncItem
    {
        public string Key { get; set; }

        public DateTimeOffset AbsoluteExpiration { get; set; }

        public ActionType ActionType { get; set; }

        public string InstanceId { get; set; }
    }

    public enum ActionType
    {
        Set = 0,
        Remove = 1
    }
}
