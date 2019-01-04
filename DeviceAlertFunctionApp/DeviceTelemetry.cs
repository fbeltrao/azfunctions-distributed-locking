using Newtonsoft.Json;

namespace DeviceAlertFunctionApp
{
    public class DeviceTelemetry
    {
        [JsonProperty("temp")]
        public double Temperature { get; set; }
    }
}