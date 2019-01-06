# AzureStorageTableLargeDataWriter
POC to implement wrapper class on top of Azure Storage Table to deal with 32KB limit per cell size enforced by Azure Storage Table

## Best result tested so far
```
Size of data: 1620891 bytes = 1582 KB
Size of data after compression: 805645 bytes = 786 KB
No of columns used: 25
```