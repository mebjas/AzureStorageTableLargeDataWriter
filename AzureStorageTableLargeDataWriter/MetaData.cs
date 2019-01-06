namespace AzureStorageTableLargeDataWriter
{
    using Newtonsoft.Json;

    /// <summary>
    /// Class to store metadata associated with splitting and compression
    /// </summary>
    internal class MetaData
    {
        /// <summary>
        /// default constructor
        /// </summary>
        public MetaData()
        {
        }

        /// <summary>
        /// constructor
        /// </summary>
        /// <param name="columnCount">no of columns used</param>
        /// <param name="length">length of compressed data</param>
        public MetaData(int columnCount, int length)
        {
            this.ColumnCount = columnCount;
            this.Length = length;
        }

        /// <summary>
        /// no of columns used
        /// </summary>
        [JsonPropertyAttribute("c")]
        public int ColumnCount { get; set; }

        /// <summary>
        /// length of compressed data
        /// </summary>
        [JsonPropertyAttribute("l")]
        public int Length { get; set; }
    }
}
