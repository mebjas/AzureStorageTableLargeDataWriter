using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace AzureStorageTableBigDataWriter
{
    class Program
    {

        static CloudTable table;

        static void Main(string[] args)
        {
            string connectionString = "";
            CloudStorageAccount storage = CloudStorageAccount.Parse(connectionString);
            CloudTableClient client = storage.CreateCloudTableClient();
            table = client.GetTableReference("testtable");
            table.CreateIfNotExists();

            Console.WriteLine("Connected to table storage");
            InsertAsync().Wait();


            Console.ReadKey();
        }

        public static async Task InsertAsync()
        {
            const int MaxLenPerColumn = 32 * 1024;
            try
            {
                DataEntity entity = new DataEntity();
                entity.PartitionKey = Guid.NewGuid().ToString();
                entity.RowKey = Guid.NewGuid().ToString();

                string data = JsonConvert.SerializeObject(GenerateObject(2000));
                Console.WriteLine("EntityLength: {0} KB", data.Length / 1024);

                MetaData meta = new MetaData();
                meta.columnCount = 0;
                double colCount = Math.Ceiling((double)data.Length / MaxLenPerColumn);
                for (int i = 0; i < colCount; i++)
                {
                    int lengthToFetch = (i == colCount - 1) ? data.Length % MaxLenPerColumn : MaxLenPerColumn;
                    entity.Add("e" + (i + 1), new EntityProperty(data.Substring(MaxLenPerColumn * i, lengthToFetch)));
                    meta.columnCount++;
                }

                string metadata = JsonConvert.SerializeObject(meta);
                entity.Add("_meta", new EntityProperty(metadata));

                TableOperation operation = TableOperation.Insert(entity);
                await table.ExecuteAsync(operation);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception");
                Console.WriteLine(ex.GetType().Name);
                Console.WriteLine(ex.Message);
            }
        }

        public static Dictionary<string, string> GenerateObject(int n)
        {
            Dictionary<string, string> obj = new Dictionary<string, string>();
            for (int i = 0; i < n; i++)
            {
                obj.Add("key_" + i, Guid.NewGuid().ToString());
            }

            return obj;
        }
    }
}
