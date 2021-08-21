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
using System.Linq;

namespace BigData.Container.Create
{
    public static class QueryResourceById
    {
        private static Guid guidContainerName;
        private static Guid guidResourceId;

        [FunctionName("QueryResourceById")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req,
            ExecutionContext context,
            ILogger log)
        {
            log.LogInformation("Query By Id Initiated");

            var _config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            string dataLakeConnectionString = _config["Values:AzureWebJobsStorage"];
            string _dataLogTrackingTable = _config["Values:AzureWebJobsLogTable"];

            string containerName = req.Query["ContainerName"];
            string resourceId = req.Query["ResourceId"];
                        
            if(!Guid.TryParse(containerName, out guidContainerName) && !Guid.TryParse(resourceId, out guidResourceId))
                return new BadRequestObjectResult("Parameter ContainerName, has to be a valid GUID");

            try
            {
                return new OkObjectResult(FindResourcesById(containerName, resourceId, dataLakeConnectionString, _dataLogTrackingTable));
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }

        }
        private static IQueryable<DataEntryLogEntity> FindResourcesById(string organizationId, string resourceId, string dataLakeConnectionString, string _dataLogTrackingTable)
        {
            CloudStorageAccount _cloudStorageAccount = CloudStorageAccount.Parse(dataLakeConnectionString);
            CloudTableClient tableClient = _cloudStorageAccount.CreateCloudTableClient(new TableClientConfiguration());
            CloudTable table = tableClient.GetTableReference(_dataLogTrackingTable);

            IQueryable<DataEntryLogEntity> linqQuery = table.CreateQuery<DataEntryLogEntity>()
            .Where(x => x.RowKey == resourceId && x.PartitionKey == organizationId)
            .Select(x => new DataEntryLogEntity() {
                ContainerName = x.ContainerName,
                ResourceId = x.ResourceId,
                ResourceType = x.ResourceType,
                Resource = x.Resource,
                UpdatedOn = x.UpdatedOn
            });

            return linqQuery;
        }
    }


}
