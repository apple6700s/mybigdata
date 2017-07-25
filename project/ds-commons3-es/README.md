
Currently based on ElasticSearch 2.3.3

### ds-commons3-es-bulkWriter

This project provides the best practice with Elasticsearch bulk action, including following features:
 
 + Client will routes the request and directly send bulk request to the primary shard, which reduces the forward cost.
 + Offers auto retry with retryable exceptions including EsRejectedException
 
### ds-commons3-es-luceneWriter-local

This project provides a LocalIndexWriter to write es data directly to local directory in lucene format.

### ds-commons3-es-luceneWriter-remote

This project provides a HdfsIndexWriter to write es data to HDFS in lucene format, and a shell script that pulls the data back to local machine with corresponding index.