using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageTableBigDataWriter
{
    class DataEntity : TableEntity
    {
        private IDictionary<string, EntityProperty> _properties;

        public DataEntity()
        {
            _properties = new Dictionary<string, EntityProperty>();
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            this._properties = properties;
        }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return _properties;
        }

        public void Add(string key, EntityProperty value)
        {
            _properties.Add(key, value);
        }
    }
}
