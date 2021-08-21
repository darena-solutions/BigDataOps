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
using Newtonsoft.Json.Linq;

namespace BigData.Container.Create
{
    public class DataEntryLogEntity : TableEntity
    {
        public DataEntryLogEntity() { }
        public DataEntryLogEntity(string containerNamePK, string resourceIdRK)
        {
            PartitionKey = containerNamePK;
            RowKey = resourceIdRK;
        }
        public string ResourceType { get; set; }
        public string Resource { get; set; }
        public string ResourceId { get; set; }
        public string ContainerName { get; set; }
        public DateTime UpdatedOn { get; set; }
    }

    public class DataReturnLogEntity
    {
        public string ResourceType { get; set; }
        public JObject Resource { get; set; }
        public string ResourceId { get; set; }
        public string ContainerName { get; set; }
        public DateTime UpdatedOn { get; set; }
    }
    public class QueryReturnDto
    {
        public string ResourceType { get; set; }
        public string ResourceId { get; set; }
        public string Resource { get; set; }
    }

}
