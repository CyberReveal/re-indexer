**Re-indexer**

This is a little tool which help with re-indexing your ELasticsearch data. This tool is based on time events.
That means the document has to contain at least one date field or _timestamp field has to be set up.



**USAGE:**
```
java -jar target/es-reindexer-1.0-SNAPSHOT.jar -cn <cluster-name> -d <destination-index> -sd <start-date> 
-f <field-to-be-used> -h <host> -i <source-index> -t <type>
```
