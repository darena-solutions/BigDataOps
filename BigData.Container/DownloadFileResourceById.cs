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
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Net.Http;
using System.Net;

namespace BigData.Container.Create
{
    public static class DownloadFileResourceById
    {
        private static Guid guidContainerName;

        [FunctionName("DownloadFileResourceById")]
        public static async Task<HttpResponseMessage> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "DownloadFileResourceById/{containerName?}/{resourceEntityId?}")] HttpRequest req,
            string containerName,
            string resourceEntityId,
            ExecutionContext context,
            ILogger log)
        {
            log.LogInformation("Trigger: QueryFileResource");

//            string _dataLakeConnectionString = Environment.GetEnvironmentVariable("Values:AzureWebJobsStorage", EnvironmentVariableTarget.Process);
//            string _uploadLogTrackingTable = Environment.GetEnvironmentVariable("Values:AzureUploadsLogTable", EnvironmentVariableTarget.Process);

            string _dataLakeConnectionString = "DefaultEndpointsProtocol=https;AccountName=darenadatalakedev1;AccountKey=3Kb3zBLtVrJw3MdkXSavBEFPQ0gS9oL28yUkTgsy1+Rlb9E+Vzi7M6ZovY+B8ZVgCVtInGKrzXQ5yTt3lcpDHA==;EndpointSuffix=core.windows.net";
            string _uploadLogTrackingTable = "FileUploadLogTable";

            DataLakeServiceClient _dataLakeServiceClient = new DataLakeServiceClient(_dataLakeConnectionString);
            DataLakeFileSystemClient _dataLakeFileSystemClient = _dataLakeServiceClient.GetFileSystemClient(containerName);

            CloudStorageAccount _cloudStorageAccount = CloudStorageAccount.Parse(_dataLakeConnectionString);
            CloudTableClient tableClient = _cloudStorageAccount.CreateCloudTableClient(new TableClientConfiguration());
            CloudTable table = tableClient.GetTableReference(_uploadLogTrackingTable);

            var linqQuery = table.CreateQuery<ResourceUploadLogEntity>()
            .Where(x => x.RowKey == resourceEntityId && x.PartitionKey == containerName)
            .Select(x => new ResourceUploadLogEntity()
            {
                ResourcePath = x.ResourcePath,
                ResourceType = x.ResourceType,
                ResourceName = x.ResourceName
            }).First();



            DataLakeFileClient file = _dataLakeFileSystemClient.GetFileClient(linqQuery.ResourcePath);

            var prop = file.GetProperties();

            Stream fileContents = file.OpenRead();
            return new HttpResponseMessage()
            {
                StatusCode = HttpStatusCode.OK,
                Content = new StreamContent(file.OpenRead())
            };

        }
        private static IQueryable<ResourceUploadLogQuery> FindResourcesById(string dataLakeConnectionString, string _dataLogTrackingTable, string containerName, string resourceId)
        {
            CloudStorageAccount _cloudStorageAccount = CloudStorageAccount.Parse(dataLakeConnectionString);
            CloudTableClient tableClient = _cloudStorageAccount.CreateCloudTableClient(new TableClientConfiguration());
            CloudTable table = tableClient.GetTableReference(_dataLogTrackingTable);

            IQueryable<ResourceUploadLogQuery> linqQuery = table.CreateQuery<ResourceUploadLogEntity>()
            .Where(x => x.RowKey.CompareTo(resourceId) >= 0 && x.PartitionKey == containerName)
            .Select(x => new ResourceUploadLogQuery()
            {
                ContainerName = x.ContainerName,
                ResourceEntityId = x.RowKey,
                ResourceType = x.ResourceType,
                ResourceName = x.ResourceName,
                ResourceStatus = x.ResourceStatus,
                CreatedOn = x.CreatedOn,
                UpdatedOn = x.UpdatedOn
            });

            return linqQuery;
        }
    }
}
