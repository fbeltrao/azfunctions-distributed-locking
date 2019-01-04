using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Net;
using System.Linq;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json;
using System.Text;
using System.Collections.Generic;
using IoTHubTrigger = Microsoft.Azure.WebJobs.EventHubTriggerAttribute;


namespace DeviceAlertFunctionApp
{
    public static class ThrottledDeviceAlertFunctions
    {
        static readonly Lazy<ThrottledGate> storageThrottledGate = new Lazy<ThrottledGate>(() =>
        {
            return new AzureStorageThrottledGate(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "device-alerts");
        });

        static readonly Lazy<ThrottledGate> cachedWithStorageBackendThrottledGate = new Lazy<ThrottledGate>(() =>
        {
            return new CachedThrottledGate(new AzureStorageThrottledGate(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "device-alerts-memory"));
        });

        static readonly Lazy<ThrottledGate> cosmosDBThrottledGate = new Lazy<ThrottledGate>(() =>
        {
            return new CosmosDBThrottledGate(Environment.GetEnvironmentVariable("CosmosDBConnectionString"), "device-alerts", "leases");
        });
        

        const double TemperatureThreshold = 25.0;

        // Minimum is 15 seconds
        const int ThrottleTimeInSeconds = 15;

        // This function will throttle alerts per device
        // It uses a Blob lease to lock a device for the throttle time
        [FunctionName(nameof(StorageThrottledDeviceAlert))]
        public static async Task<IActionResult> StorageThrottledDeviceAlert(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "storage/{deviceId}")] HttpRequest req,
            string deviceId,
            ExecutionContext executionContext,
            ILogger log)
        {
            req.Query.TryGetValue("notification", out var notification);

            return await storageThrottledGate.Value.RunAsync<IActionResult>(deviceId, TimeSpan.FromSeconds(ThrottleTimeInSeconds), executionContext,
                async () =>
                {
                    await SendNotificationAsync(deviceId, notification);
                    return new OkObjectResult(new { status = "ok", notification = notification.FirstOrDefault() });
                },
                () =>
                {
                    return Task.FromResult<IActionResult>(new OkObjectResult(new { status = "throttled" }));
                }
                );
        }


        // This function will throttle alerts per device
        // It uses a CosmosDB to lock a device for the throttle time
        [FunctionName(nameof(CosmosDBThrottledDeviceAlert))]
        public static async Task<IActionResult> CosmosDBThrottledDeviceAlert(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "cosmosdb/{deviceId}")] HttpRequest req,
            string deviceId,
            ExecutionContext executionContext,
            ILogger log)
        {
            req.Query.TryGetValue("notification", out var notification);

            return await cosmosDBThrottledGate.Value.RunAsync<IActionResult>(deviceId, TimeSpan.FromSeconds(ThrottleTimeInSeconds), executionContext,
                async () =>
                {
                    await SendNotificationAsync(deviceId, notification);
                    return new OkObjectResult(new { status = "ok", notification = notification.FirstOrDefault() });
                },
                () =>
                {
                    return Task.FromResult<IActionResult>(new OkObjectResult(new { status = "throttled" }));
                }
                );
        }


        // This function will throttle alerts per device
        // It uses a Blob lease to lock a device for the throttle time
        [FunctionName(nameof(InMemoryThrottledDeviceAlert))]
        public static async Task<IActionResult> InMemoryThrottledDeviceAlert(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = "memory/{deviceId}")] HttpRequest req,
            string deviceId,
            ExecutionContext executionContext,
            ILogger log)
        {
            req.Query.TryGetValue("notification", out var notification);

            return await cachedWithStorageBackendThrottledGate.Value.RunAsync<IActionResult>(deviceId, TimeSpan.FromSeconds(ThrottleTimeInSeconds), executionContext,
                async () =>
                {
                    await SendNotificationAsync(deviceId, notification);
                    return new OkObjectResult(new { status = "ok", notification = notification.FirstOrDefault() });
                },
                () =>
                {
                    return Task.FromResult<IActionResult>(new OkObjectResult(new { status = "throttled" }));
                }
                );
        }



        [FunctionName(nameof(IoTHubDeviceTelemetryListener))]
        public static async Task IoTHubDeviceTelemetryListener(
            [IoTHubTrigger("messages/events", Connection = "iothub_connectionstring")] EventData[] events,
            ExecutionContext executionContext,
            ILogger log)
        {
            var notificationTasks = new List<Task>();
            foreach (var eventData in events)
            {
                
                var deviceId = eventData.SystemProperties["iothub-connection-device-id"].ToString();
                var message = JsonConvert.DeserializeObject<DeviceTelemetry>(Encoding.UTF8.GetString(eventData.Body));
                if (message.Temperature >= TemperatureThreshold)
                {
                    // notify that the device temperature is too high
                    var notificationTask = cachedWithStorageBackendThrottledGate.Value.RunAsync(
                        deviceId, 
                        TimeSpan.FromMinutes(1), 
                        executionContext,                
                        async () => 
                        {
                            await SendNotificationAsync(deviceId, $"Temperature too high: {message.Temperature}");
                        });

                    notificationTasks.Add(notificationTask);
                }
            }

            // if there are any notifications, wait for them to finish
            if (notificationTasks.Count > 0)
                await Task.WhenAll(notificationTasks);
        }

        private static Task SendNotificationAsync(string deviceId, string notification)
        {
            if (string.Equals(notification, "fail", StringComparison.InvariantCultureIgnoreCase))
                throw new Exception("Error sending notification");

            Console.WriteLine($"Notification for {deviceId} sent.");
            return Task.FromResult(0);
        }
    }
}
