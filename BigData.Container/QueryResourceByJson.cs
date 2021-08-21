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
using System.Data.SqlClient;

namespace BigData.Container.Create
{
    public static class QueryResourceByJson
    {
        private static Guid guidContainerName;
        public static SqlConnectionStringBuilder builder;

        [FunctionName("QueryResourceByJson")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = "QueryResourceByJson/{containerName?}/{resourceType?}")] HttpRequest req,
            string containerName,
            string resourceType,
            ExecutionContext context,
            ILogger log)
        {
            log.LogInformation("Create Resource Object Initiated");

            var _config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: false, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            ConfigureSynapseConnection(_config["Values:SynapzeConnString"]);

            string queryString = req.QueryString.ToString();

            try
            {
                return new OkObjectResult(GetQueryData(containerName, resourceType, queryString));
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message);
            }

        }

        private static void ConfigureSynapseConnection(string synapseConnectionString)
        {
            Dictionary<string, string> synapseConn = new Dictionary<string, string>();
            foreach (var item in synapseConnectionString.Split(';'))
            {
                var x = item.Split('=');
                synapseConn.Add(x[0], x[1]);
            }
            builder = new SqlConnectionStringBuilder
            {
                DataSource = synapseConn["DataSource"],
                UserID = synapseConn["UserID"],
                Password = synapseConn["Password"],
                InitialCatalog = synapseConn["InitialCatalog"],
                MultipleActiveResultSets = true
            };
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

        public static List<QueryReturnDto> GetQueryData(string OrganizationId, string ResourceTypeTag, string queryString)
        {
           List<QueryReturnDto> queryResult = new List<QueryReturnDto>();

            using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
            {
                string searchPath = GenerateQueryFilesPath(OrganizationId, ResourceTypeTag);
                Dictionary<string, string> conditionalParse = GenerateWhereClause(queryString);

                string sql = "SELECT JSON_VALUE(jsonContent,'$.resourceType') as ResourceType, JSON_VALUE(jsonContent,'$.id') as ResourceId,  jsonContent " +
                    string.Format("FROM  OPENROWSET( BULK '{0}',", searchPath) +
                    "FORMAT = 'CSV', " +
                    "FIELDQUOTE = '0x0b', " +
                    "FIELDTERMINATOR = '0x0b', " +
                    "ROWTERMINATOR = '\n' ) " +
                    "WITH( jsonContent varchar(MAX) ) AS[result] " +
                    conditionalParse["From"] +
                    "WHERE 1=1" +
                    conditionalParse["Clause"];

                connection.Open();

                using (SqlCommand command = new SqlCommand(sql, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            QueryReturnDto qResp = new QueryReturnDto()
                            {
                                ResourceType = reader.GetString(0),
                                ResourceId = reader.GetString(1),
                                Resource = reader.GetString(2)
                            };
                            queryResult.Add(qResp);
                        }
                    }
                }
            }
            return queryResult;
        }

        // Need to change the Hardcoded URL based on the connection string.
        private static string GenerateQueryFilesPath(string OrganizationId, string ResourceTypeTag)
        {
            if (OrganizationId == string.Empty || OrganizationId == null)
                OrganizationId = "*";
            if (ResourceTypeTag == string.Empty || ResourceTypeTag == null)
                ResourceTypeTag = "*";


            return string.Format("https://darenadatalakedev1.blob.core.windows.net/{0}/*/{1}.json", OrganizationId, ResourceTypeTag);
        }
        private static Dictionary<string, string> GenerateWhereClause(string queryString)
        {
            Dictionary<string, string> From = new Dictionary<string, string>();
            Dictionary<string, string> FromClause = new Dictionary<string, string>();
            FromClause["Clause"] = string.Empty;
            FromClause["From"] = string.Empty;

            string clause = string.Empty;

            if (queryString == string.Empty || queryString == null)
                return FromClause;

            if (queryString.StartsWith('?'))
                queryString = queryString.TrimStart('?');

            foreach (var itm in queryString.Split('&'))
            {
                string[] i = itm.Split('=');
                if (i[0].Contains('[') && i[0].Contains(']'))
                {
                    string tmpDepth = string.Empty;
                    string currRoot = "jsonContent";
                    string currPath = string.Empty;
                    int lvlExplosion = 0;

                    foreach (var lvl in i[0].Split('.'))
                    {
                        currPath += "." + lvl;

                        if (lvl.EndsWith("[]"))
                        {
                            if (!From.ContainsKey(currPath))
                            {
                                lvlExplosion++;
                                tmpDepth += "." + lvl.Replace("[", "").Replace("]", "");

                                string crossJoin = string.Format("CROSS APPLY OPENJSON (JSON_QUERY({0},'${1}')) AS T{2} ", currRoot, tmpDepth, lvlExplosion);
                                FromClause["From"] += crossJoin;
                                currRoot = string.Format("T{0}.value", lvlExplosion);
                                tmpDepth = string.Empty;
                                From.Add(currPath, currRoot);
                            }
                        }
                        else
                        {
                            tmpDepth += "." + lvl;

                        }
                    }
                    clause += string.Format("AND JSON_VALUE({0}, '${1}') = '{2}' ", currRoot, tmpDepth, i[1]);
                    FromClause["Clause"] += clause;
                }
                else
                {
                    clause += string.Format("AND JSON_VALUE(jsonContent, '$.{0}') = '{1}' ", i[0], i[1]);
                    FromClause["Clause"] += string.Format("AND JSON_VALUE(jsonContent, '$.{0}') = '{1}' ", i[0], i[1]);
                }
            }
            //return clause;
            return FromClause;

        }
    }

}
