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

namespace BigData.Container.Create
{
    public static class UploadFileResource
    {
        private static Guid guidContainerName;

        [FunctionName("UploadFileResource")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "UploadFileResource/{containerName?}/{resourceId?}")] HttpRequest req,
            string containerName,
            string resourceId,
            ExecutionContext context,
            ILogger log)
        {
            log.LogInformation("Trigger: UploadFileResource");

//            string _dataLakeConnectionString = Environment.GetEnvironmentVariable("Values:AzureWebJobsStorage", EnvironmentVariableTarget.Process);
//            string _uploadLogTrackingTable = Environment.GetEnvironmentVariable("Values:AzureUploadsLogTable", EnvironmentVariableTarget.Process);
//            string _uploadRepoPath = Environment.GetEnvironmentVariable("Values:AzureUploadsRepository", EnvironmentVariableTarget.Process);

            string _dataLakeConnectionString = "DefaultEndpointsProtocol=https;AccountName=darenadatalakedev1;AccountKey=3Kb3zBLtVrJw3MdkXSavBEFPQ0gS9oL28yUkTgsy1+Rlb9E+Vzi7M6ZovY+B8ZVgCVtInGKrzXQ5yTt3lcpDHA==;EndpointSuffix=core.windows.net";
            string _uploadLogTrackingTable = "FileUploadLogTable";
            string _uploadRepoPath = "upload-repository/Patients/{0}/PatientUpload/{1}";

            DataLakeServiceClient _dataLakeServiceClient = new DataLakeServiceClient(_dataLakeConnectionString);
            DataLakeFileSystemClient _dataLakeFileSystemClient = _dataLakeServiceClient.GetFileSystemClient(containerName);

            List<string> resp = new List<string>();
            bool allPass = true;
            var formdata = await req.ReadFormAsync();

            foreach (IFormFile file in formdata.Files)
            {
                string resPath = GenerateStoragePathPatientUploads(_uploadRepoPath, resourceId, file.FileName);

                var r = UploadFileToLake(_dataLakeFileSystemClient, file, resPath);

                resp.Add(string.Format("File:{0}; Status:{1}", file.FileName, r.ToString()));

                if (!r)
                    allPass = false;
                await MakeUploadLogEntry(_dataLakeConnectionString, _uploadLogTrackingTable, containerName, resourceId, file.FileName, file.ContentType, "Upload:"+r.ToString(), resPath);
            }
            if (allPass)
            {
                return new OkObjectResult(resp);
            }
            else 
            {
                return new BadRequestObjectResult(resp);
            }

        }
        private static string GenerateStoragePathPatientUploads(string _uploadRepoPath, string resourceId, string fileName)
        {
            return string.Format(_uploadRepoPath, resourceId, fileName);
        }

        public static bool UploadFileToLake(DataLakeFileSystemClient _dataLakeFileSystemClient, IFormFile formFile, string storePath)
        {
            try
            {
                MemoryStream stream = new MemoryStream();

                DataLakeFileClient dataLakeFileClient = _dataLakeFileSystemClient.CreateFile(storePath);

                formFile.CopyTo(stream);

                stream.Position = 0;

                dataLakeFileClient.Upload(stream, overwrite: true);

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        private static async Task MakeUploadLogEntry(string dataLakeConnectionString, string _Table, string containerName, string resourceId, string resourceName, string resourceType,string Status, string resourcePath)
        {
            CloudStorageAccount _cloudStorageAccount = CloudStorageAccount.Parse(dataLakeConnectionString);
            CloudTableClient tableClient = _cloudStorageAccount.CreateCloudTableClient(new TableClientConfiguration());
            CloudTable table = tableClient.GetTableReference(_Table);

            ResourceUploadLogEntity uploadLogEntity = new ResourceUploadLogEntity(containerNamePK: containerName.ToString(), resourceIdRK: resourceId+ "_"+ Guid.NewGuid().ToString())
            {
                ContainerName = containerName.ToString(),
                ResourceId = resourceId ,
                ResourceType = resourceType,
                ResourceName = resourceName,
                ResourceStatus = Status,
                ResourcePath = resourcePath,
                CreatedOn = DateTime.UtcNow,
                UpdatedOn = DateTime.UtcNow
            };
            TableOperation insertOperation = TableOperation.InsertOrMerge(uploadLogEntity);
            TableResult result = await table.ExecuteAsync(insertOperation);
        }

    }

}
