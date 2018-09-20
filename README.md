# ElasticSearch CRUD with Rollover

This project focuses mainly on demo-ing rollover Java API usage with ElasticSearch.

# Prerequisites

Install <a href="https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.4.0.tar.gz">ElasticSearch-6.4.0</a>, and start the elasticSearch server.

# Running

Import this project as a maven project and build using mvn clean install. Either run directly from your IDE(by right clicking on ESTester main class and clicking in Run ESTester.main().You need to edit configuration to provide command line argument/s.
The command line argument should be atleast one or atmost nine arguments in total. The argument minimum value is 1 maximum value is 9 and represents the below operations:

* 1 - Create,
* 2 - Read,
* 3 - Update, 
* 4 - Delete,
* 5 - Read Multiple,
* 6 - Insert Multiple,
* 7 - Search,
* 8 - Search All and
* 9 - Read Using Scroll.

For Example: java -jar elasticsearch-rollover-1.0.0.jar 1 3 (or) java -Xms250m -Xmx250m -jar elasticsearch-rollover-1.0.0.jar 1 3

