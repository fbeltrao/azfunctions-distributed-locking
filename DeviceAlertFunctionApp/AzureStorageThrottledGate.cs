using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Net;
using System.Threading.Tasks;

namespace DeviceAlertFunctionApp
{
    /// <summary>
    /// Azure blob lease based implementation of Throttled gate
    /// </summary>
    public class AzureStorageThrottledGate : ThrottledGate
    {
        private readonly CloudBlobContainer containerReference;

        public AzureStorageThrottledGate(string storageConnectionString, string containerName)
        {
            this.containerReference = CloudStorageAccount.Parse(storageConnectionString).CreateCloudBlobClient().GetContainerReference(containerName);
        }

        protected internal override async Task ReleaseLeaseAsync(string id, string leaseId)
        {
            try
            {
                var blob = this.containerReference.GetAppendBlobReference(id);

                // try to adquire lease
                await blob.ReleaseLeaseAsync(new AccessCondition { LeaseId = leaseId });
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.Conflict || ex.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
            {
            }
        }


        /// <summary>
        /// Tries to adquire a lease
        /// </summary>
        /// <param name="id"></param>
        /// <param name="throttleTime"></param>
        /// <param name="leaseId"></param>
        /// <returns></returns>
        protected internal override async Task<bool> TryAdquireLeaseAsync(string id, TimeSpan throttleTime, string leaseId)
        {
            // TODO: make this smarter, call only if the container does not exists
            await this.containerReference.CreateIfNotExistsAsync();


            // create blob if not exists
            var blob = this.containerReference.GetAppendBlobReference(id);
            try
            {
                await blob.CreateOrReplaceAsync(
                    AccessCondition.GenerateIfNotExistsCondition(),
                    null,
                    null);
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.Conflict || ex.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
            {
                // ignore exception caused by conflict as it means the file already exists
            }
        

            try
            {
                // try to adquire lease
                await blob.AcquireLeaseAsync(
                    throttleTime,
                    leaseId,
                    new AccessCondition { LeaseId = null },
                    null,
                    null
                    );

                // we leased the file!
                return true;
            }
            catch (StorageException ex) when (ex.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.Conflict || ex.RequestInformation?.HttpStatusCode == (int)HttpStatusCode.PreconditionFailed)
            {
                // if a conflict is returned it means that the blob has already been leased
                // in that case the operation failed (someone else has the lease)
                return false;
            }
        }
    }
}
