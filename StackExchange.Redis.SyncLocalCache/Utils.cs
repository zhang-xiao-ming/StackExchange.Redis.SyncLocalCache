using System.IO;
using System.Net;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text.RegularExpressions;

namespace StackExchange.Redis.SyncLocalCache
{
    public static class Utils
    {
        /// <summary>
        /// 检测IP地址规则的正则表达式
        /// </summary>
        private const string Pattern =
            @"^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])$";

        private static string _localIp = null;
        private static string GetLocalIp()   //获取本地IP 
        {
            if (_localIp != null) return _localIp;
            IPAddress[] ipAddrs = Dns.GetHostAddresses(Dns.GetHostName());
            string ip = string.Empty;
            foreach (IPAddress ipAddr in ipAddrs)
            {
                ip = ipAddr.ToString();
                if (IsIp(ip))
                    break;
            }
            _localIp = ip;
            return _localIp;

        }

        /// <summary>
        /// 本机IP
        /// </summary>
        public static string LocalIp
        {
            get { return _localIp ?? (_localIp = GetLocalIp()); }
        }

        /// <summary>
        /// IP地址验证的正则表达式
        /// </summary>
        private static readonly Regex PatternRegex = new Regex(Pattern);

        public static bool IsIp(string ip)
        {
            return !string.IsNullOrWhiteSpace(ip) && PatternRegex.IsMatch(ip.Trim());
        }

        public static byte[] BinarySerialize(object obj)
        {
            if (obj == null)
                return null;
            byte[] bytes = null;
            using (MemoryStream ms = new MemoryStream())
            {
                new BinaryFormatter().Serialize(ms, obj);
                bytes = ms.ToArray();
            }
            return bytes;

        }

        public static object BinaryDeserialize(byte[] butter)
        {
            if (butter == null || butter.Length <= 0)
                return null;
            using (MemoryStream ms = new MemoryStream(butter))
            {
                return new BinaryFormatter().Deserialize(ms);
            }
        }
    }
}
