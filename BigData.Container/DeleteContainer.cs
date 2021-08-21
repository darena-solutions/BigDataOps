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


namespace BigData.Container.Create
{
    public static class DeleteContainer
    {
        private static Guid guidContainerName;

        [FunctionName("DeleteContainer")]
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

            DataLakeServiceClient _dataLakeServiceClient = new DataLakeServiceClient(dataLakeConnectionString);

            string containerName = req.Query["ContainerName"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);

            containerName = containerName ?? data?.ContainerName;

            if (containerName == null || containerName == "")
                return new BadRequestObjectResult("Parameter ContainerName, cannot be null!");

            bool isValid = Guid.TryParse(containerName, out guidContainerName);
            if(!isValid)
                return new BadRequestObjectResult("Parameter ContainerName, has to be a valid GUID");

            DataLakeFileSystemClient _dataLakeFileSystemClient = _dataLakeServiceClient.GetFileSystemClient(containerName);

            try
            {
                await _dataLakeFileSystemClient.DeleteAsync();
                return new OkObjectResult(true);
            }
            catch(Exception ex)
            {
                return new OkObjectResult(ex.Message);
            }
        }
    }
}
