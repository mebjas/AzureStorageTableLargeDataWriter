namespace AzureStorageTableLargeDataWriter
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Diagnostics;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using Newtonsoft.Json;

    public class StorageTableWriter
    {
        #region constant fields
        const string MetaDataColumnName = "_md";
        const string ColumnNamePrefix = "_e";
        const int StorageTableSingleCellSize = 32 * 1024;
        const int MaxSupportedColumns = 25;
        #endregion

        #region private variables
        CloudTable table;
        #endregion

        public StorageTableWriter(string connectionString)
        {
            CloudStorageAccount storage = CloudStorageAccount.Parse(connectionString);
            CloudTableClient client = storage.CreateCloudTableClient();
            this.table = client.GetTableReference("testtable");
            this.table.CreateIfNotExists();
        }

        public async Task InsertAsync(Dictionary<string, string> entityToInsert, string columnName)
        {
            DataEntity entity = new DataEntity(entityToInsert);
            string data = entityToInsert[columnName];
            entity.Remove(columnName);
            byte[] compressedData = Compression.Zip(data);

#if DEBUG
            Debug.WriteLine("Size of data: {0} bytes = {1} KB", data.Length, data.Length / 1024);
            Debug.WriteLine("Size of data after compression: {0} bytes = {1} KB", compressedData.Length, compressedData.Length / 1024);
#endif

            MetaData meta = new MetaData(0, compressedData.Length);
            double colsNeeded = Math.Ceiling((double)compressedData.Length / StorageTableSingleCellSize);
            if (colsNeeded > MaxSupportedColumns)
            {
                throw new InvalidOperationException("Max supported size of data post compression is 800KB (25 columns)");
            }

            for (int i = 0; i < colsNeeded; i++)
            {
                int lengthToFetch = (i == colsNeeded - 1) ? compressedData.Length % StorageTableSingleCellSize : StorageTableSingleCellSize;
                entity.Add(ColumnNamePrefix + (i + 1), new EntityProperty(compressedData.Skip(StorageTableSingleCellSize * i).Take(lengthToFetch).ToArray()));
                meta.ColumnCount++;
            }

#if DEBUG
            Debug.WriteLine("No of columns used: {0}", meta.ColumnCount);
#endif

            string metadata = JsonConvert.SerializeObject(meta);
            entity.Add(MetaDataColumnName, new EntityProperty(metadata));
            TableOperation operation = TableOperation.Insert(entity);
            await table.ExecuteAsync(operation).ConfigureAwait(false);
        }

        public async Task<Dictionary<string, string>> RetrieveAsync(string partitionKey, string rowKey, string columnName)
        {
            Dictionary<string, string> response = new Dictionary<string, string>();

            TableOperation operation = TableOperation.Retrieve<DataEntity>(partitionKey, rowKey);
            TableResult retrievedResult = await this.table.ExecuteAsync(operation).ConfigureAwait(false);

            if (retrievedResult == null || retrievedResult.Result == null)
            {
                return null;
            }

            var result = ((DataEntity)retrievedResult.Result);
            if (!(result.ContainsKey(MetaDataColumnName)))
            {
                throw new InvalidOperationException("Invalid format, no metadata found");
            }

            MetaData md = JsonConvert.DeserializeObject<MetaData>(result[MetaDataColumnName].StringValue);
            result.Remove(MetaDataColumnName);

            byte[] dataArray = new byte[md.Length];
            for (int i = 0; i < md.ColumnCount; i++)
            {
                result[ColumnNamePrefix + (i + 1)].BinaryValue.CopyTo(dataArray, i * StorageTableSingleCellSize);
                result.Remove(ColumnNamePrefix + (i + 1));
            }

            response[columnName] = Compression.Unzip<string>(dataArray);

            //// Move all remaining properties to the return value
            foreach (KeyValuePair<string, EntityProperty> kvp in result)
            {
                if (kvp.Key != columnName)
                {
                    response[kvp.Key] = kvp.Value.StringValue;
                }
            }

            response[DataEntity.PartitionKeyName] = result.PartitionKey;
            response[DataEntity.RowKeyName] = result.RowKey;
            return response;
        }
    }
}
