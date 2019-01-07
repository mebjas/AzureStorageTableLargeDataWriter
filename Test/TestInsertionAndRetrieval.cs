namespace Test
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading.Tasks;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using AzureStorageTableLargeDataWriter;
    using Newtonsoft.Json;

    [TestClass]
    public class TestInsertionAndRetrieval
    {
        static string connectionString = "";
        static Dictionary<string, string> randomKVP = new Dictionary<string, string>();

        [TestMethod]
        public async Task TestMethod1()
        {
            const int n = 32000;
            string partitionKey = Guid.NewGuid().ToString();
            string rowKey = Guid.NewGuid().ToString();
            string columnName = "largeDataColumn";

            try
            {
                StorageTableWriter writer = new StorageTableWriter(connectionString);
                Dictionary<string, string> data = GenerateObject(partitionKey, rowKey, columnName, n);
                await writer.InsertAsync(data, columnName);
                Dictionary<string, string> readData = await writer.RetrieveAsync(partitionKey, rowKey, columnName);
                Assert.IsNotNull(readData);

                Dictionary<string, string> largeData = JsonConvert.DeserializeObject<Dictionary<string, string>>(readData[columnName]);
                Assert.AreEqual(n, largeData.Count);

                foreach (KeyValuePair<string, string> kvp  in largeData)
                {
                    Assert.IsTrue(randomKVP.ContainsKey(kvp.Key));
                    Assert.AreEqual(randomKVP[kvp.Key], kvp.Value);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine("Exception Type: {0}", ex.GetType().Name);
                Debug.WriteLine("Exception Message: {0}", ex.Message);

                Assert.Fail();
                throw;
            }
        }

        public static Dictionary<string, string> GenerateObject(string partitionKey, string rowKey, string columnName, int n)
        {
            Dictionary<string, string> obj = new Dictionary<string, string>();
            obj.Add("PartitionKey", partitionKey);
            obj.Add("RowKey", rowKey);

            for (int i = 0; i < n; i++)
            {
                randomKVP["key_" +i] = Guid.NewGuid().ToString();
            }

            obj.Add(columnName, JsonConvert.SerializeObject(randomKVP));
            return obj;
        }
    }
}
