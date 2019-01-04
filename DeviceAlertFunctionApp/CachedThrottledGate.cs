using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Net;
using System.Threading.Tasks;

namespace DeviceAlertFunctionApp
{
    /// <summary>
    /// Caching of throttled gate, minimizing access to backend
    /// </summary>
    public class CachedThrottledGate : ThrottledGate
    {
        static ILogger<CachedThrottledGate> logger = new LoggerFactory().CreateLogger<CachedThrottledGate>();
        static object cachedObject = new object();
        private readonly IMemoryCache cache;
        private readonly ThrottledGate concrete;
        private readonly string cachePrefix;
        private readonly TimeSpan timeoutForFailedLeases;

        public CachedThrottledGate(ThrottledGate concrete, IMemoryCache cache = null, string cachePrefix = "throttle-leases-", TimeSpan? timeoutForFailedLeases = null)
        {
            this.cache = cache ?? new MemoryCache(new MemoryCacheOptions());
            this.concrete = concrete;
            this.cachePrefix = cachePrefix;
            this.timeoutForFailedLeases = timeoutForFailedLeases ?? TimeSpan.FromSeconds(3);
        }

        protected internal override async Task ReleaseLeaseAsync(string id, string leaseId)
        {
            await this.concrete.ReleaseLeaseAsync(id, leaseId);

            this.cache.Remove(CreateCacheKey(id));
        }

        private string CreateCacheKey(string id) => string.Concat(this.cachePrefix, id);


        /// <summary>
        /// Tries to adquire a lease
        /// </summary>
        /// <param name="id"></param>
        /// <param name="throttleTime"></param>
        /// <param name="leaseId"></param>
        /// <returns></returns>
        protected internal override async Task<bool> TryAdquireLeaseAsync(string id, TimeSpan throttleTime, string leaseId)
        {
            var cacheKey = CreateCacheKey(id);
            // If we haven't find in cached we need to try concrete implementation
            if (!this.cache.TryGetValue(cacheKey, out _))
            {
                var leaseAdquired = await this.concrete.TryAdquireLeaseAsync(id, throttleTime, leaseId);
                if (leaseAdquired)
                {
                    // lease adquired, save it in cache with the timeout
                    this.cache.Set<object>(cacheKey, cachedObject, DateTimeOffset.UtcNow.Add(throttleTime));
                    return true;
                }
                else
                {
                    // lease not adquired, since we don't know long if we need to wait we add cached value with a pre-defined timeout
                    // default is 3 seconds
                    this.cache.Set<object>(cacheKey, cachedObject, DateTimeOffset.UtcNow.Add(this.timeoutForFailedLeases));
                }
            }

            return false;
        }
    }
}
