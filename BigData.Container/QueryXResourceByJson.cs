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
using Newtonsoft.Json.Linq;

namespace BigData.Container.Create
{
    public static class QueryXResourceByJson
    {
        private static Guid guidContainerName;
        public static SqlConnectionStringBuilder builder;
        private static Dictionary<string, string> operatorMap;

        [FunctionName("QueryXResourceByJson")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "QueryXResourceByJson/{containerName?}/{resourceType?}")] HttpRequest req,
            string containerName,
            string resourceType,
            ExecutionContext context,
            ILogger log)
        {
            log.LogInformation("Create Resource Object Initiated");
            
            var _config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            //string dataLakeConnectionString = _config["Values:AzureWebJobsStorage"];
            //string _dataLogTrackingTable = _config["Values:AzureWebJobsLogTable"];
            //string _synapzeConnString = _config["Values:SynapzeConnString"];
            
            string _dataLogTrackingTable = Environment.GetEnvironmentVariable("AzureWebJobsLogTable", EnvironmentVariableTarget.Process);
            string _synapzeConnString = Environment.GetEnvironmentVariable("SynapzeConnString", EnvironmentVariableTarget.Process);

            ConfigureSynapseConnection(_synapzeConnString);
            operatorMap = new Dictionary<string, string>();

            operatorMap.Add("eq", "=");
            operatorMap.Add("ne", "!=");
            operatorMap.Add("gt", ">");
            operatorMap.Add("lt", "<");
            operatorMap.Add("ge", ">=");
            operatorMap.Add("le", "<=");


            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            var queryJson = JObject.Parse(requestBody);

            try
            {
                return new OkObjectResult(await GetQueryDataAsync(containerName, resourceType, queryJson));
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

        public static async Task<List<QueryReturnDto>> GetQueryDataAsync(string OrganizationId, string ResourceTypeTag, JObject queryString)
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
                    "WHERE 1=1 " +
                    conditionalParse["Clause"];

                connection.Open();

                using (SqlCommand command = new SqlCommand(sql, connection))
                {
                    using (SqlDataReader reader = await command.ExecuteReaderAsync())
                    {
                        while (reader.Read())
                        {
                            QueryReturnDto qResp = new QueryReturnDto()
                            {
                                ResourceType = reader.GetString(0),
                                ResourceId = reader.GetString(1),
                                Resource = JObject.Parse(reader.GetString(2))
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



        private static Dictionary<string, string> GenerateWhereClause(JObject queryString)
        {
            Dictionary<string, string> From = new Dictionary<string, string>();
            Dictionary<string, string> FromClause = new Dictionary<string, string>();
            FromClause["Clause"] = string.Empty;
            FromClause["From"] = string.Empty;

            string clause = string.Empty;

            if (queryString == null && queryString["QueryClause"] != null && queryString["QuerySelect"] != null)
            {
                return FromClause;
            }

            var ClauseObject = queryString["QueryClause"];

            string FinalCLauseString = string.Empty;

            // Need a better way to Identify if the Payload is  Single or Nested Operation
            if (ClauseObject.ToString().Contains("opertions"))
            {
                FinalCLauseString = AutoGenerateWhereClause(ref From, ref FromClause, ClauseObject);
            }
            else
            {
                FinalCLauseString = GenerateSingleComparisonOp((JObject)ClauseObject, ref From, ref FromClause);
            }
            if (FinalCLauseString != string.Empty)
                FromClause["Clause"] = " AND ";

            FromClause["Clause"] += FinalCLauseString;
          
            return FromClause;

        }

        private static string AutoGenerateWhereClause(ref Dictionary<string, string> From, ref Dictionary<string, string> FromClause, JToken ClauseObject)
        {
            List<string> tmpConditionList = new List<string>();

            foreach (var item in ClauseObject["operations"])
            {
                if ((string)item["opType"] == "Single")
                {
                    tmpConditionList.Add(GenerateSingleComparisonOp((JObject)item, ref From, ref FromClause));
                }
                else
                {
                    tmpConditionList.Add(AutoGenerateWhereClause(ref From, ref FromClause, item));
                }

            }
            return "(" + String.Join((string)ClauseObject["opMode"], tmpConditionList) + ")";
        }

        private static string GenerateSingleComparisonOp(JObject clauseObj, ref Dictionary<string, string> From, ref Dictionary<string, string> FromClause)
        {
            SingleCompOperation sOP = JsonConvert.DeserializeObject<SingleCompOperation>(clauseObj.ToString());

            string clause = string.Empty;
            if(sOP.opType == "Single")
            {

                    if (sOP.parameter.Contains('[') && sOP.parameter.Contains(']'))
                    {
                        string tmpDepth = string.Empty;
                        string currRoot = "jsonContent";
                        string currPath = string.Empty;
                        int lvlExplosion = 0;

                        foreach (var lvl in sOP.parameter.Split('.'))
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
                        switch (sOP.dataType)
                        {
                            case "string":
                                return string.Format(" JSON_VALUE({0}, '${1}') {2} '{3}' ", currRoot, tmpDepth, operatorMap[sOP.operand], sOP.value);
                            case "datetime":
                                return string.Format(" CAST(JSON_VALUE({0}, '${1}') as DATETIME) {2} '{3}' ", currRoot, tmpDepth, operatorMap[sOP.operand], sOP.value);
                            case "date":
                                return string.Format(" CAST(JSON_VALUE({0}, '${1}') as DATE) {2} '{3}' ", currRoot, tmpDepth, operatorMap[sOP.operand], sOP.value);
                            case "int":
                                return string.Format(" CAST(JSON_VALUE({0}, '${1}') as INT) {2} '{3}' ", currRoot, tmpDepth, operatorMap[sOP.operand], sOP.value);
                            case "float":
                                return string.Format(" CAST(JSON_VALUE({0}, '${1}') as FLOAT) {2} '{3}' ", currRoot, tmpDepth, operatorMap[sOP.operand], sOP.value);
                            default:
                                return string.Format(" JSON_VALUE({0}, '${1}') {2} '{3}' ", currRoot, tmpDepth, operatorMap[sOP.operand], sOP.value);
                        }
                    }
                    else
                    {
                        switch (sOP.dataType)
                        {
                            case "string":
                                return string.Format(" JSON_VALUE(jsonContent, '$.{0}') {1} '{2}' ", sOP.parameter, operatorMap[sOP.operand], sOP.value);
                            case "datetime":
                                return string.Format(" CAST(JSON_VALUE(jsonContent, '$.{0}') AS DATETIME) {1} '{2}' ", sOP.parameter, operatorMap[sOP.operand], sOP.value);
                            case "date":
                                return string.Format(" CAST(JSON_VALUE(jsonContent, '$.{0}') AS DATE) {1} '{2}' ", sOP.parameter, operatorMap[sOP.operand], sOP.value);
                            case "int":
                                return string.Format(" CAST(JSON_VALUE(jsonContent, '$.{0}') AS INT) {1} '{2}' ", sOP.parameter, operatorMap[sOP.operand], sOP.value);
                            case "float":
                                return string.Format(" CAST(JSON_VALUE(jsonContent, '$.{0}') AS FLOAT) {1} '{2}' ", sOP.parameter, operatorMap[sOP.operand], sOP.value);
                            default:
                                return string.Format(" JSON_VALUE(jsonContent, '$.{0}') {1} '{2}' ", sOP.parameter, operatorMap[sOP.operand], sOP.value);
                        }
                    }
            }
            return clause;
        }
        public class SingleCompOperation
        {
            public string opType { get; set; }
            public string dataType { get; set; }
            public string operand { get; set; }
            public string parameter { get; set; }
            public string value { get; set; }
            public string options { get; set; }

        }
    }

}
