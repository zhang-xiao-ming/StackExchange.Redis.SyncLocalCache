using StackExchange.Redis;
using StackExchange.Redis.SyncLocalCache;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.Caching;
using System.Threading;
using System.Threading.Tasks;

namespace SyncLocalCacheTest
{
    internal class Program
    {
        private static readonly List<string> AllKeys = new List<string>();
        private const int KeyNumber = 1000;
        private const int ThreadNum = 10;
        private static readonly List<string> SearchKeys = new List<string>(ThreadNum);
        private static MemoryCacheSync _memoryCacheSync = null;
        private static void Main(string[] args)
        {
            try
            {
                MemoryCacheSync.Default.Logger = (string message, Exception ex) =>
                {
                    Console.WriteLine(string.Format("{0},{1}", message, ex.ToString()));
                };

                //TestSearch();
                TestSync();
                
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
            finally
            {
                Thread.Sleep(1000);
                Console.ReadLine();
            }
        }

        private static MemoryCacheSync CreateMemoryCacheSync()
        {
            if (_memoryCacheSync != null)
                return _memoryCacheSync;
            string configString = "172.16.101.71:6379,172.16.101.72:6379";
            ConfigurationOptions options = ConfigurationOptions.Parse(configString);
            var conn = ConnectionMultiplexer.Connect(options);
            conn.PreserveAsyncOrder = false;
            MemoryCacheSync cache = new MemoryCacheSync(conn, MemoryCache.Default, "TestApp1")
            {
                Logger =
                    (string message, Exception ex) =>
                    {
                        Console.WriteLine(string.Format("{0},{1}", message, ex.ToString()));
                    }
            };
            _memoryCacheSync = cache;
            return cache;
        }

        private static void TestSync()
        {
            try
            {
                //MemoryCacheSync cache = CreateMemoryCacheSync();
                MemoryCacheSync cache = MemoryCacheSync.Default;
                Console.WriteLine("InstanceId:{0},AppId:{1}", cache.InstanceId, cache.AppId);
                while (true)
                {
                    string text = Console.ReadLine();
                    if (text == null)
                        continue;
                    string[] array = text.Split(' ');
                    string cmd = array[0];
                    string key = array.Length >= 2 ? array[1] : "";
                    string value = array.Length >= 3 ? array[2] : "";
                    if (cmd.Equals("set"))
                    {
                        cache.Set(key, value, new TimeSpan(1, 0, 0));
                    }
                    if (cmd.Equals("get"))
                    {
                        Console.WriteLine(cache.Get(key));
                    }
                    if (cmd.Equals("remove"))
                    {
                        cache.Remove(key);
                    }
                    if (cmd != "status") continue;
                    IDatabase db = cache.ConnectionMultiplexer.GetDatabase();
                    IEnumerable<RedisValue> list = db.SetScan(cache.AppId);
                    var redisValues = list as RedisValue[] ?? list.ToArray();
                    Console.WriteLine(redisValues.Count());
                    foreach (RedisValue item in redisValues)
                    {
                        Console.WriteLine(item);
                        string totalQueryCountKey = string.Format("{0}-{1}-{2}", cache.AppId, item, "TotalQueryCount");
                        string totalLocalHitCountKey = string.Format("{0}-{1}-{2}", cache.AppId, item, "TotalLocalHitCount");
                        string totalRedisHitCountKey = string.Format("{0}-{1}-{2}", cache.AppId, item, "TotalRedisHitCount");
                        Console.WriteLine("{0}:{1}", totalQueryCountKey, db.StringGet(totalQueryCountKey));
                        Console.WriteLine("{0}:{1}", totalLocalHitCountKey, db.StringGet(totalLocalHitCountKey));
                        Console.WriteLine("{0}:{1}", totalRedisHitCountKey, db.StringGet(totalRedisHitCountKey));
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
        }

        private static void TestSearch()
        {
            try
            {
                for (int i = 0; i < KeyNumber; i++)
                {
                    string key = Guid.NewGuid().ToString();
                    AllKeys.Add(key);
                }
                Random rand = new Random(1000);
                for (int i = 0; i < ThreadNum; i++)
                {
                    string key = AllKeys[rand.Next(0, KeyNumber - 1)];
                    SearchKeys.Insert(i, key);
                    Console.WriteLine("key:" + key);
                }

                TestGetOrAdd();

                TestGetOrAddUseKeyLock();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private static void TestGetOrAdd()
        {
            try
            {

                Console.WriteLine("TestGetOrAdd:");
                //MemoryCacheSync cache = CreateMemoryCacheSync();
                MemoryCacheSync cache = MemoryCacheSync.Default;
                foreach (string key in AllKeys)
                {
                    cache.Remove(key);
                }

                Stopwatch watch = new Stopwatch();
                watch.Start();
                List<Task> taskList = new List<Task>();
                for (int i = 0; i < ThreadNum; i++)
                {
                    string key = SearchKeys[i];
                    taskList.Add(Task.Factory.StartNew(() =>
                    {
                        object value = cache.GetOrAdd(key, () =>
                        {
                            Thread.Sleep(2000);
                            Console.WriteLine(key);
                            return key;
                        }, new TimeSpan(0, 0, 5));
                        Console.WriteLine(value);
                    }));
                }
                Task.WaitAll(taskList.ToArray());
                watch.Stop();

                Console.WriteLine("time:" + watch.ElapsedMilliseconds);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        private static void TestGetOrAddUseKeyLock()
        {
            try
            {
                Console.WriteLine("TestGetOrAddUseKeyLock:");
                //MemoryCacheSync cache = CreateMemoryCacheSync();
                MemoryCacheSync cache = MemoryCacheSync.Default;
                foreach (string key in AllKeys)
                {
                    cache.Remove(key);
                }
                Stopwatch watch = new Stopwatch();
                watch.Start();
                List<Task> taskList = new List<Task>();
                for (int i = 0; i < ThreadNum; i++)
                {
                    string key = SearchKeys[i];
                    taskList.Add(Task.Factory.StartNew(() =>
                    {
                        object value = cache.GetOrAddUseKeyLock(key, () =>
                        {
                            Thread.Sleep(2000);
                            Console.WriteLine(key);
                            return key;
                        }, new TimeSpan(0, 0, 5));

                    }));
                }
                Task.WaitAll(taskList.ToArray());
                watch.Stop();
                Console.WriteLine("time:" + watch.ElapsedMilliseconds);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }
    }
}
