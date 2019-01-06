namespace AzureStorageTableBigDataWriter
{
    using System.Threading.Tasks;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    class StorageTableWriter
    {
        const string MetaDataColumnName = "_md";
        const string ColumnNamePrefix = "e";
        const int StorageTableSingleCellSize = 32 * 1024;

        CloudTable table;

        public StorageTableWriter(string connectionString)
        {
            CloudStorageAccount storage = CloudStorageAccount.Parse(connectionString);
            CloudTableClient client = storage.CreateCloudTableClient();
            this.table = client.GetTableReference("testtable");
            this.table.CreateIfNotExists();
        }

        public async Task InsertAsync(DataEntity entity, string columnName)
        {

        }
    }

    internal class MetaData
    {
        public int columnCount { get; set; }
    }
}
