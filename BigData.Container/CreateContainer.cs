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
using Microsoft.Azure.Cosmos.Table;


namespace BigData.Container.Create
{
    public static class CreateContainer
    {
        private static Guid guidContainerName;

        [FunctionName("CreateContainer")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ExecutionContext context,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            var _config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            string dataLakeConnectionString = _config["Values:AzureWebJobsStorage"];
            string _dataLogTrackingTable = _config["Values:AzureWebJobsLogTable"];

            DataLakeServiceClient _dataLakeServiceClient = new DataLakeServiceClient(dataLakeConnectionString);

            string containerName = req.Query["ContainerName"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            log.LogInformation(requestBody);

            containerName = containerName ?? data?.ContainerName;

            if (containerName == null || containerName == "")
                return new BadRequestObjectResult("Parameter ContainerName, cannot be null!");

            bool isValid = Guid.TryParse(containerName, out guidContainerName);
            if(!isValid)
                return new BadRequestObjectResult("Parameter ContainerName, has to be a valid GUID");

            log.LogInformation(containerName);


            try
            {
                DataLakeFileSystemClient _dataLakeFileSystemClient = _dataLakeServiceClient.GetFileSystemClient(containerName);

                CloudStorageAccount _cloudStorageAccount =CloudStorageAccount.Parse(dataLakeConnectionString);
                CloudTableClient tableClient = _cloudStorageAccount.CreateCloudTableClient(new TableClientConfiguration());
                CloudTable table = tableClient.GetTableReference(_dataLogTrackingTable);

                await _dataLakeFileSystemClient.CreateAsync(Azure.Storage.Files.DataLake.Models.PublicAccessType.FileSystem);
                await table.CreateIfNotExistsAsync();
                
                return new OkObjectResult(true);
            }
            catch(Exception ex)
            {
                return new OkObjectResult(ex.Message);
            }
        }
    }
}
