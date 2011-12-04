A JPA implementation for Apache Cassandra.

This release is only compatible with 0.8.1 forward.  Cassandra now natively supports DynamicComposites.

#Notices

As of version hector-jpa-0.8.7-1, the secondary indexing is incompabile with previous revisions.  This is due to a bug in the row key construction
that could potentially result in 2 indexes sharing a row.  This issue has been resolved, but has changed the row key for secondary indexing.  In order to rebuild
the indexes, each entity will need to be loaded and re-saved (merge).  This can be done using the hector KeyIterator object
to load each entity row.



# What this plugin supports

* Direct field to column mapping

* One-To-* Ordered and Unordered Collections
* Many-To-* Ordered and Unordered Collections
* Entity cascading based on cascade behavior
* Inerhitance
* Basic Secondary indexing with inheritance


# What this plugin does not support

* OR query clauses
* IN and other query apis currently unsupported by Cassandra


# Roadmap

As cassandra matures, this plugin will as well.  Below is the roadmap for these features

* Continue working with OpenJPA to make this part of the Open JPA Core
* Convert client from Hector Thrift to CQL JDBC compliant driver
* Remove client site composite row secondary indexing and use the Distributed Lucene Index.  This depends on this issue.  https://issues.apache.org/jira/browse/CASSANDRA-2915


# Querying

Querying requires the user to define secondary indexes on Entities.  See the UserDateRange and Sale classes in the test cases for examples.
This plugin requires build time enhancement.  This is due to the startup time analysis for inheritance in the query engine. As CQL improves, this limitation can be removed.

