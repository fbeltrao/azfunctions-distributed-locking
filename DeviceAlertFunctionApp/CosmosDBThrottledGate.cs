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

        private readonly CosmosContainer container;

        public CosmosDBThrottledGate(CosmosClient cosmosClient, string databaseName, string containerName)
        {
            var db = cosmosClient.Databases[databaseName];
            this.container = db.Containers[containerName];
           
        }
        public CosmosDBThrottledGate(string cosmosDbConnectionString, string databaseName, string containerName)
        {
            var cosmosClient = new Microsoft.Azure.Cosmos.CosmosClient(cosmosDbConnectionString);
            var db = cosmosClient.Databases[databaseName];
            this.container = db.Containers[containerName];
        }

        protected internal override async Task ReleaseLeaseAsync(string id, string leaseId)
        {
            try
            {
                // Reads the document with the lease
                // If the lease is expired, try to update the document
                var readResponse = await this.container.Items.ReadItemAsync<CosmosDBLease>(id, id, new CosmosItemRequestOptions() { });
                if (readResponse.StatusCode == HttpStatusCode.OK && readResponse.Resource.LeaseID == leaseId)
                {
                    var newLease = new CosmosDBLease()
                    {
                        ID = id,
                        LeaseID = null,
                        LeasedUntil = null,
                    };

                    var replaceItemResponse = await this.container.Items.ReplaceItemAsync(id, id, newLease, new CosmosItemRequestOptions() {
                        AccessCondition = new AccessCondition()
                        {
                            Type = AccessConditionType.IfMatch,
                            Condition = readResponse.ETag,
                        }
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
            // Document is already there, check if it has an expired lease
            var readResponse = await this.container.Items.ReadItemAsync<CosmosDBLease>(id, id);
            if (readResponse.StatusCode == HttpStatusCode.OK)
            {
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
                    var updateLeaseResponse = await this.container.Items.ReplaceItemAsync(
                        id, 
                        id, 
                        updatedLease,
                        new CosmosItemRequestOptions()
                        {
                            AccessCondition = new AccessCondition()
                            {
                                Type = AccessConditionType.IfMatch,
                                Condition = readResponse.ETag,
                            },
                        });
                    
                    return updateLeaseResponse.StatusCode == HttpStatusCode.OK;
                }
                catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
                {
                    // someone else leased before us
                    
                }

                return false;
            }

            // Document did not exist. Try to be the first to created it, adquiring the lease
            var newLease = new CosmosDBLease()
            {
                ID = id,
                LeaseID = leaseId,
                LeasedUntil = DateTime.UtcNow.Add(throttleTime),
            };

            try
            {
                var createResponse = await this.container.Items.CreateItemAsync<CosmosDBLease>(
                    id, 
                    newLease);
                
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
