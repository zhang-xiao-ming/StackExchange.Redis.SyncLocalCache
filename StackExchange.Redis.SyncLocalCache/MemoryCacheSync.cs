using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Runtime.Caching;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis.SyncLocalCache
{
    public class MemoryCacheSync
    {
        public ConnectionMultiplexer ConnectionMultiplexer { get; private set; }
        public ObjectCache ObjectCache { get; private set; }
        public string AppId { get; private set; }

        public string InstanceId
        {
            get { return string.Format("{0}:{1}", Utils.LocalIp, Process.GetCurrentProcess().Id); }
        }
        public Action<string, Exception> Logger { get; set; }
        private long _totalQueryCount;
        private long _totalLocalHitCount;
        private long _totalRedisHitCount;
        private Timer _timer = null;

        private static MemoryCacheSync _instance = null;
        private static readonly object SyncRoot = new object();

        public static MemoryCacheSync Default
        {
            get
            {
                if (_instance == null)
                {
                    lock (SyncRoot)
                    {
                        if (_instance == null)
                        {
                            _instance = new MemoryCacheSync();
                        }
                    }
                }
                return _instance;
            }
        }

        public MemoryCacheSync(ConnectionMultiplexer connectionMultiplexer, ObjectCache cache, string appId)
        {
            Init(connectionMultiplexer, cache, appId);
        }

        public MemoryCacheSync()
            : this(ConfigurationManager.AppSettings["Redis.Connection"], ConfigurationManager.AppSettings["AppId"])
        {

        }

        public MemoryCacheSync(string configuration, string appId)
        {
            ConfigurationOptions options = ConfigurationOptions.Parse(configuration);
            ConnectionMultiplexer connectionMultiplexer = ConnectionMultiplexer.Connect(options);
            connectionMultiplexer.PreserveAsyncOrder = false;
            Init(connectionMultiplexer, MemoryCache.Default, appId);
        }

        private void Init(ConnectionMultiplexer connectionMultiplexer, ObjectCache cache, string appId)
        {
            if (connectionMultiplexer == null)
                throw new ArgumentNullException("connectionMultiplexer");
            ConnectionMultiplexer = connectionMultiplexer;
            if (cache == null)
                throw new ArgumentNullException("cache");
            ObjectCache = cache;

            if (string.IsNullOrWhiteSpace(appId))
            {
                appId = ConfigurationManager.AppSettings["AppId"];
            }
            if (string.IsNullOrWhiteSpace(appId))
                throw new ArgumentNullException("appId");
            AppId = appId;
            RegisterSyncCacheSubscribe();
            _timer = new Timer((object state) =>
            {
                try
                {
                    if (ConnectionMultiplexer == null || !ConnectionMultiplexer.IsConnected)
                        return;
                    IDatabase db = ConnectionMultiplexer.GetDatabase();
                    db.SetAdd(AppId, InstanceId);
                    string totalQueryCountKey = string.Format("{0}-{1}-{2}", AppId, InstanceId, "TotalQueryCount");
                    string totalLocalHitCountKey = string.Format("{0}-{1}-{2}", AppId, InstanceId, "TotalLocalHitCount");
                    string totalRedisHitCountKey = string.Format("{0}-{1}-{2}", AppId, InstanceId, "TotalRedisHitCount");

                    db.StringSet(totalQueryCountKey, _totalQueryCount);
                    db.StringSet(totalLocalHitCountKey, _totalLocalHitCount);
                    db.StringSet(totalRedisHitCountKey, _totalRedisHitCount);
                }
                catch (Exception ex)
                {
                    try
                    {
                        if (Logger != null)
                        {
                            Logger("MemoryCacheSync.TimerCallback", ex);
                        }
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }, null, 0, 10000);
        }
        private static string MergeKey(string appId, string key)
        {
            return string.Format("{0}-{1}", appId, key);
        }

        private void WriteLog(string message, Exception ex)
        {
            try
            {
                if (Logger != null)
                {
                    Logger(message, ex);
                }
            }
            catch
            {
                // ignored
            }
        }

        private void RegisterSyncCacheSubscribe()
        {
            if (ConnectionMultiplexer == null || !ConnectionMultiplexer.IsConnected)
                return;
            ISubscriber sub = ConnectionMultiplexer.GetSubscriber();
            IDatabase db = ConnectionMultiplexer.GetDatabase();
            sub.SubscribeAsync(AppId, (channel, message) =>
            {
                try
                {
                    SyncItem syncItem = (SyncItem)Utils.BinaryDeserialize(message);
                    if (syncItem == null || syncItem.InstanceId == InstanceId || syncItem.Key == null)
                        return;
                    if (syncItem.ActionType == ActionType.Remove)
                        ObjectCache.Remove(syncItem.Key);

                    if (syncItem.ActionType != ActionType.Set) return;
                    byte[] bytes = db.StringGet(syncItem.Key);
                    object obj = Utils.BinaryDeserialize(bytes);
                    if (obj == null) return;
                    ObjectCache.Set(syncItem.Key, obj, syncItem.AbsoluteExpiration.AddMilliseconds(new Random((int)DateTime.Now.Ticks).Next(5, 5000)));
                }
                catch (Exception ex)
                {
                    WriteLog("MemoryCacheSync.RegisterSyncCacheSubscribe", ex);
                }
            });
        }

        public object GetOrAdd(string key, Func<object> getValue, TimeSpan? expiry = null)
        {
            try
            {
                object value = Get(key);
                if (value != null || getValue == null) return value;
                value = getValue();
                Set(key, value, expiry);
                return value;
            }
            catch (Exception ex)
            {
                WriteLog("MemoryCacheSync.GetOrSet", ex);
            }
            return null;
        }

        public object GetOrAddUseKeyLock(string key, Func<object> getValue, TimeSpan? expiry = null)
        {
            bool useKeyLock = false;
            try
            {
                object value = Get(key);
                if (value != null || getValue == null) return value;
                lock (KeyLock.Lock(key))
                {
                    useKeyLock = true;
                    value = Get(key);
                    if (value != null)
                        return value;
                    value = getValue();
                    Set(key, value, expiry);
                    return value;
                }
            }
            catch (Exception ex)
            {
                WriteLog("MemoryCacheSync.GetOrAddUseKeyLock", ex);
                return null;
            }
            finally
            {
                try
                {
                    if (useKeyLock)
                        KeyLock.Unlock(key);
                }
                catch (Exception)
                {
                    // ignored
                }
            }
        }

        public void Set(string key, object value, TimeSpan? expiry = null)
        {
            try
            {

                if (string.IsNullOrEmpty(key) || value == null)
                    return;
                key = MergeKey(AppId, key);
                DateTimeOffset absoluteExpiration = expiry == null
                    ? DateTime.MaxValue
                    : DateTimeOffset.Now.Add(expiry.Value);
                ObjectCache.Set(key, value, absoluteExpiration);
                if (ConnectionMultiplexer == null || !ConnectionMultiplexer.IsConnected)
                    return;
                Task.Factory.StartNew(() =>
                {
                    try
                    {
                        IDatabase db = ConnectionMultiplexer.GetDatabase();
                        db.StringSet(key, Utils.BinarySerialize(value), expiry);
                        ISubscriber sub = ConnectionMultiplexer.GetSubscriber();
                        byte[] msg = Utils.BinarySerialize(new SyncItem()
                        {
                            Key = key,
                            AbsoluteExpiration = absoluteExpiration,
                            ActionType = ActionType.Set,
                            InstanceId = InstanceId
                        });
                        sub.PublishAsync(AppId, msg);
                    }
                    catch (Exception ex)
                    {
                        WriteLog("MemoryCacheSync.Set.Redis", ex);
                    }
                });
            }
            catch (Exception ex)
            {
                WriteLog("MemoryCacheSync.Set", ex);
            }
        }

        public object Get(string key)
        {
            try
            {
                if (string.IsNullOrEmpty(key))
                    return null;
                Interlocked.Increment(ref _totalQueryCount);
                key = MergeKey(AppId, key);
                var value = ObjectCache.Get(key);
                if (value != null)
                {
                    Interlocked.Increment(ref _totalLocalHitCount);
                    return value;
                }

                try
                {
                    if (ConnectionMultiplexer == null || !ConnectionMultiplexer.IsConnected)
                        return null;

                    IDatabase db = ConnectionMultiplexer.GetDatabase();
                    byte[] bytes = db.StringGet(key);
                    value = Utils.BinaryDeserialize(bytes);
                    if (value != null)
                    {
                        Interlocked.Increment(ref _totalRedisHitCount);
                    }
                    return value;
                }
                catch (Exception ex)
                {
                    WriteLog("MemoryCacheSync.Get.Redis", ex);
                    return null;
                }
            }
            catch (Exception ex)
            {
                WriteLog("MemoryCacheSync.Get", ex);
                return null;
            }
        }

        public void Remove(string key)
        {
            try
            {
                if (ConnectionMultiplexer == null || string.IsNullOrEmpty(key))
                    return;
                key = MergeKey(AppId, key);
                ObjectCache.Remove(key);
                IDatabase db = ConnectionMultiplexer.GetDatabase();
                db.KeyDelete(key);
                ISubscriber sub = ConnectionMultiplexer.GetSubscriber();
                byte[] msg = Utils.BinarySerialize(new SyncItem() { Key = key, ActionType = ActionType.Remove });
                sub.PublishAsync(AppId, msg);
            }
            catch (Exception ex)
            {
                WriteLog("MemoryCacheSync.Remove", ex);
            }
        }

    }

    internal static class KeyLock
    {
        private static readonly object SyncRoot = new object();
        private static readonly Dictionary<string, object> Locks = new Dictionary<string, object>();
        public static object Lock(string key)
        {
            object lockObject;
            Locks.TryGetValue(key, out lockObject);
            if (lockObject == null)
            {
                lock (SyncRoot)
                {
                    Locks.TryGetValue(key, out lockObject);
                    if (lockObject == null)
                    {
                        lockObject = new object();
                        Locks[key] = lockObject;
                    }
                }
            }
            return lockObject;
        }

        public static void Unlock(string key)
        {
            lock (SyncRoot)
            {
                if (Locks.ContainsKey(key))
                    Locks.Remove(key);
            }
        }
    }

    public class NullValue
    {
    }
}
