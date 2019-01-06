namespace AzureStorageTableLargeDataWriter
{
    using System;
    using System.Collections.Generic;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;

    public class DataEntity : TableEntity
    {
        public const string PartitionKeyName = "PartitionKey";

        public const string RowKeyName = "RowKey";

        private IDictionary<string, EntityProperty> _properties;

        public DataEntity()
        {
            this._properties = new Dictionary<string, EntityProperty>();
        }

        public DataEntity(Dictionary<string, string> entity)
        {
            if (!entity.ContainsKey(PartitionKeyName))
            {
                throw new ArgumentException(string.Format("{0} property is needed", PartitionKeyName));
            }

            if (!entity.ContainsKey(RowKeyName))
            {
                throw new ArgumentException(string.Format("{0} property is needed", RowKeyName));
            }


            this._properties = new Dictionary<string, EntityProperty>();
            foreach(KeyValuePair<string, string> kvp in entity)
            {
                this._properties.Add(kvp.Key, new EntityProperty(kvp.Value));
            }
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            this._properties = properties;
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return this._properties;
        }

        public void Add(string key, EntityProperty value)
        {
            this._properties.Add(key, value);
        }

        public void Add(string key, string value)
        {
            this._properties.Add(key, new EntityProperty(value));
        }

        public void Remove(string key)
        {
            this._properties.Remove(key);
        }

        public EntityProperty Get(string key)
        {
            return this._properties[key];
        }

        public bool ContainsKey(string key)
        {
            return this._properties.ContainsKey(key);
        }

        public EntityProperty this[string key]
        {
            get
            {
                return this.Get(key);
            }

            set
            {
                this.Add(key, value);
            }
        }

        public IEnumerator<KeyValuePair<string, EntityProperty>> GetEnumerator()
        {
            return this._properties.GetEnumerator();
        }
    }
}
