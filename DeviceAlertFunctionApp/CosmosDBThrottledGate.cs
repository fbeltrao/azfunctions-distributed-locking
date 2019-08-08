using Microsoft.Azure.Cosmos;
using Microsoft.Azure.WebJobs;
using Newtonsoft.Json;
using System;
using System.Net;
using System.Threading.Tasks;

namespace DeviceAlertFunctionApp
{
    /// <summary>
    /// Cosmos DB lease based implementation of Throttled gate
    /// </summary>
    public class CosmosDBThrottledGate : ThrottledGate
    {
        
        public class CosmosDBLease
        {
            [JsonProperty("id")]
            public string ID {get;set;}

            [JsonProperty("leasedUntil")]
            public DateTime? LeasedUntil {get;set;}

            [JsonProperty("leaseId")]
            public string LeaseID {get;set;}


        }

        private readonly Container container;

        public CosmosDBThrottledGate(CosmosClient cosmosClient, string databaseName, string containerName)
        {
            var db = cosmosClient.GetDatabase(databaseName);
            this.container = db.GetContainer(containerName);
           
        }
        public CosmosDBThrottledGate(string cosmosDbConnectionString, string databaseName, string containerName)
        {
            var cosmosClient = new Microsoft.Azure.Cosmos.CosmosClient(cosmosDbConnectionString);
            var db = cosmosClient.GetDatabase(databaseName);
            this.container = db.GetContainer(containerName);
        }

        protected internal override async Task ReleaseLeaseAsync(string id, string leaseId)
        {
            try
            {
                var partitionKey = new PartitionKey(id);
                // Reads the document with the lease
                // If the lease is expired, try to update the document
                var readResponse = await this.container.ReadItemAsync<CosmosDBLease>(id, partitionKey);
                if (readResponse.StatusCode == HttpStatusCode.OK && readResponse.Resource.LeaseID == leaseId)
                {
                    var newLease = new CosmosDBLease()
                    {
                        ID = id,
                        LeaseID = null,
                        LeasedUntil = null,
                    };

                    var replaceItemResponse = await this.container.ReplaceItemAsync<CosmosDBLease>(newLease, id, partitionKey, new ItemRequestOptions() {
                        IfMatchEtag = readResponse.ETag,
                    });
                }
            }
            catch (Exception)
            {
                throw;
            }
        }


        /// <summary>
        /// Tries to acquire a lease
        /// </summary>
        /// <param name="id"></param>
        /// <param name="throttleTime"></param>
        /// <param name="leaseId"></param>
        /// <returns></returns>
        protected internal override async Task<bool> TryAcquireLeaseAsync(string id, TimeSpan throttleTime, string leaseId)
        {
            var partitionKey = new PartitionKey(id);

            // Document is already there, check if it has an expired lease
            ItemResponse<CosmosDBLease> readResponse;
            try
            {
                readResponse = await this.container.ReadItemAsync<CosmosDBLease>(id, partitionKey);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Does not exist: create it!
                return await TryCreateLeaseAsync(id, leaseId, throttleTime);
            }

            var existingLease = readResponse.Resource;
            if (existingLease.LeasedUntil != null && existingLease.LeasedUntil.Value >= DateTime.UtcNow)
                return false;

            var updatedLease = new CosmosDBLease()
            {
                ID = id,
                LeaseID = leaseId,
                LeasedUntil = DateTime.UtcNow.Add(throttleTime),
            };

            try
            {
                var updateLeaseResponse = await this.container.ReplaceItemAsync<CosmosDBLease>(
                    updatedLease,
                    id,
                    partitionKey,
                    new ItemRequestOptions()
                    {
                        IfMatchEtag = readResponse.ETag,
                    });

                return updateLeaseResponse.StatusCode == HttpStatusCode.OK;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                // someone else leased before us
                return false;
            }
        }

        private async Task<bool> TryCreateLeaseAsync(string id, string leaseId, TimeSpan throttleTime)
        {
            // Document did not exist. Try to be the first to created it, adquiring the lease
            var newLease = new CosmosDBLease()
            {
                ID = id,
                LeaseID = leaseId,
                LeasedUntil = DateTime.UtcNow.Add(throttleTime),
            };

            try
            {
                var createResponse = await this.container.CreateItemAsync<CosmosDBLease>(
                    newLease,
                    new PartitionKey(id));

                if (createResponse.StatusCode == HttpStatusCode.Created)
                    return true;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                // document was created before, we did not get the lease
                // do not throw exception
            }

            return false;
        }
    }
}
