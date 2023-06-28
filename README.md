StackOverflow Data Analysis using Distributed K means clustering 
--------------------------
Final Project for CS6240 Summer 2023

Author
-----------
- Nidhi Satish Pai
- Srijha Kalyan

Installation
------------
These components are installed:
- OpenJDK 11
- Hadoop 3.3.5
- Maven (Tested with version 3.6.3)
- AWS CLI (Tested with version 1.22.34)

- Scala 2.12.17 (you can install this specific version with the Coursier CLI tool which also needs to be installed)
- Spark 3.3.2 (without bundled Hadoop)
- Scala Build Tool sbt

After downloading the spark installations, move them to an appropriate directory:

`mv hadoop-3.3.5 /usr/local/hadoop-3.3.5`

`mv spark-3.3.2-bin-without-hadoop /usr/local/spark-3.3.2-bin-without-hadoop`

Environment
-----------
1) Example ~/.bash_aliases:
	```
	export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
	export HADOOP_HOME=/usr/local/hadoop-3.3.5
	export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
	export SCALA_HOME=/usr/share/scala
	export SPARK_HOME=/usr/local/spark-3.3.2-bin-without-hadoop
	export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SCALA_HOME/bin:$SPARK_HOME/bin
	export SPARK_DIST_CLASSPATH=$(hadoop classpath)
	```

2) Explicitly set `JAVA_HOME` in `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:

	`export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`

Execution
---------
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) Edit the Makefile to customize the environment at the top.
	Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
5) Standalone Hadoop:
	- `make switch-standalone`		-- set standalone Hadoop environment (execute once)
	- `make local`
6) Pseudo-Distributed Hadoop: (https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation)
	- `make switch-pseudo`			-- set pseudo-clustered Hadoop environment (execute once)
	- `make pseudo`					-- first execution
	- `make pseudoq`				-- later executions since namenode and datanode already running 
7) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	- `make make-bucket`			-- only before first execution
	- `make upload-input-aws`		-- only before first execution
	- `make aws`					-- check for successful execution with web interface (aws.amazon.com)
	- `download-output-aws`		-- after successful execution & termination

How to run the code
-----------
for installing sbt visit [sbt reference manual](https://www.scala-sbt.org/1.x/docs/Setup.html)

At the root of the project run `sbt`

```bash
sbt run
```

Project Objective
-----------
The goal of this project is to develop a data analysis solution that applies the k-means clustering algorithm to a dataset of Stack Overflow posts. The purpose is to group the posts based on their programming language, allowing for easier analysis and insights into programming trends.
The project utilizes Apache Spark, a distributed computing framework, to handle large-scale data processing. The Spark RDD (Resilient Distributed Dataset) abstraction is leveraged to perform transformations and computations on the dataset in a distributed and parallel manner. The resulting clusters provide insights into programming language trends and preferences within the Stack Overflow community.
The project successfully implements k-means clustering using Spark RDDs, showcasing its ability to handle extensive datasets. The analysis includes data loading and parsing, grouping of questions and answers, scoring, vectorization, and the clustering process. The output includes clusters with dominant programming languages, the percentage of questions in each language, cluster sizes, and median scores. The project demonstrates effective data processing and provides valuable insights for understanding programming trends in the Stack Overflow community.


About the Dataset
-----------
This is a simplified version of a StackOverflow dataset. Each row represents a StackOverflow post and is formatted as follows:
<item>
•	postTypeId: It represents the type of the post. 1 for questions and 2 for answers.
•	id: This is the unique ID of the post.
•	acceptedAnswer: (For question posts only) The ID of the accepted answer for a particular question.
•	parentId: (For answer posts only) The ID of the question that this answer post is associated with.
•	score: This represents the score of the post (the difference between upvotes and downvotes).
•	tags: (For question posts only) This is a string representing the tags associated with the question.
</item>

Each line in the provided text file has the following format: 
<postTypeId>,<id>,[<acceptedAnswer>],[<parentId>],<score>,[<tag>]

Results
-------

1. Parallelize the computation for each K, running this algorithm for a single K-value at a time
   Results: k = 45
   ![image](https://github.com/CS-6240-2023-Summer-1/project-spark-kmeans-clustering/assets/50697493/9fb7ab77-ca8d-4f4f-8d4e-c2927d44ef9b)


   
3. Running the algorithm for many K-values in parallel, but executing each complete clustering for a single K-value “sequentially.” 


