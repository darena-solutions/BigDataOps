using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;
using Azure.Storage.Files.DataLake;
using System.Text;
using Microsoft.Azure.Cosmos.Table;

namespace BigData.Container.Create
{
    public static class CreateResource
    {
        private static Guid guidContainerName;

        [FunctionName("CreateResource")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            ExecutionContext context,
            ILogger log)
        {
            log.LogInformation("Create Resource Object Initiated");

            var _config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            string dataLakeConnectionString = _config["Values:AzureWebJobsStorage"];
            string _dataLogTrackingTable = _config["Values:AzureWebJobsLogTable"];


            string containerName = req.Query["ContainerName"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic resource = JsonConvert.DeserializeObject(requestBody);

            string resourceId = string.Empty;
            string resourceType = string.Empty;
            resourceId = resource.SelectToken("$.id").ToString();
            resourceType = resource.SelectToken("$.resourceType").ToString();

            if (resourceId == null || resourceType == null)
                return new BadRequestObjectResult("Missing resourceType or resourceId information in the json object");

            bool isValid = Guid.TryParse(containerName, out guidContainerName);
            if(!isValid)
                return new BadRequestObjectResult("Parameter ContainerName, has to be a valid GUID");

            try
            {
                await StoreResourceInDatalake(dataLakeConnectionString, containerName, resource, resourceType);

                await MakeDateLogEntry(dataLakeConnectionString, _dataLogTrackingTable, containerName, resource, resourceId, resourceType);

                return new OkObjectResult(true);
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }

        }

        private static async Task StoreResourceInDatalake(string dataLakeConnectionString, string containerName, dynamic resource, string resourceType)
        {
            DataLakeServiceClient _dataLakeServiceClient = new DataLakeServiceClient(dataLakeConnectionString);
            DataLakeFileSystemClient _dataLakeFileSystemClient = _dataLakeServiceClient.GetFileSystemClient(containerName);
            DataLakeFileSystemClient _fileSystemClient = _dataLakeServiceClient.GetFileSystemClient(containerName);


            string currentDateDirectory = DateTime.Now.ToString("yyyyMMdd");
            DataLakeDirectoryClient directoryClient = await _fileSystemClient.CreateDirectoryAsync(currentDateDirectory);

            DataLakeFileClient dataLakeFileClient = directoryClient.GetFileClient(resourceType + ".json");
            _ = await dataLakeFileClient.CreateIfNotExistsAsync();


            Stream stream = new MemoryStream();

            byte[] byteArray = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(resource));

            stream.Write(byteArray, 0, byteArray.Length);

            byte[] byteNL = Encoding.UTF8.GetBytes(Environment.NewLine);

            stream.Write(byteNL, 0, byteNL.Length);

            stream.Position = 0;

            long currentLength = dataLakeFileClient.GetProperties().Value.ContentLength;
            long fileSize = stream.Length;

            await dataLakeFileClient.AppendAsync(stream, offset: currentLength);
            await dataLakeFileClient.FlushAsync(position: currentLength + fileSize);
        }

        private static async Task MakeDateLogEntry(string dataLakeConnectionString, string _dataLogTrackingTable, string containerName, dynamic resource, string resourceId, string resourceType)
        {
            CloudStorageAccount _cloudStorageAccount = CloudStorageAccount.Parse(dataLakeConnectionString);
            CloudTableClient tableClient = _cloudStorageAccount.CreateCloudTableClient(new TableClientConfiguration());
            CloudTable table = tableClient.GetTableReference(_dataLogTrackingTable);

            DataEntryLogEntity dataEntryLogEntity = new DataEntryLogEntity(containerNamePK: containerName.ToString(), resourceIdRK: resourceId)
            {
                ContainerName = containerName.ToString(),
                ResourceId = resourceId,
                ResourceType = resourceType,
                Resource = JsonConvert.SerializeObject(resource),
                UpdatedOn = DateTime.UtcNow
            };
            TableOperation insertOperation = TableOperation.InsertOrMerge(dataEntryLogEntity);
            TableResult result = await table.ExecuteAsync(insertOperation);
        }
    }
}
