This is a ReadMe file which contains the instructions for running the following JAVA files on HADOOP.

The files are:
1. Driver.java
2. TotalPages.java
3. LinkGraph.java
4. FinalPageRank.java
5. FinalSortedRank.java

							----- Brief Description of all the java files. -----

1. Driver.java - This file contains all the jobs that will run the subsequent java files. There are 4 jobs in it:
		 Job1 - This will run the TotalPages.java class.
		 Job2 - This will run the LinkGraph.java class.
		 Job3 - This will run the FinalPageRank.java class.
		 Job4 - This will run the FinalSortedRank.java class.

2. TotalPages.java - This class will give the total number of pages that are there in the input file.

3. LinkGraph.java - This class will generate the link graph of the input which is of the form (Page Outklinks1@@@@@Outlink2#####InitialPageRank)

Note: There can be more than 2 outlinks above one is just is an example.

4. FinalPageRank.java - This class will operate on the link graph of the input and will generate the final page rank for all the pages in the input wiki.

5. FinalSortedRank.java - This will sort all the page ranks in the descending order.

							----- All the commands are to be provided in the terminal. -----

Basic instructions for all files:

Set the Hadoop class path using the below commands.
1. export HADOOP_CLASSPATH=$(hadoop classpath)  - This will export your hadoop path in HADOOP_CLASSPATH.
2. echo $HADOOP_CLASSPATH                       - This will print your hadoop class path on the terminal.

							----- Instructions for Driver.java file  ----- 

1. Compile your .java file using the following command:

[cloudera@quickstart PageRank]$ javac -classpath {$HADOOP_CLASSPATH} -d '/home/cloudera/Desktop/HW-3/PageRank/' '/home/cloudera/Desktop/HW-3/Driver.java' '/home/cloudera/Desktop/HW-3/FinalPageRank.java' '/home/cloudera/Desktop/HW-3/LinkGraph.java' '/home/cloudera/Desktop/HW-3/TotalPages.java' '/home/cloudera/Desktop/HW-3/FinalSortedRank.java'


In the above command the first path after -d is the folder in which the .class file for the same will be created after the compilation, the later paths are of the  other .java files which needs to be compiled.

2. Once the .class file is created for the all the classes, we need to build a jar file for the same which will be containing the .class files, to create the .jar file below is the command:

[cloudera@quickstart PageRank]$ jar -cvf Driver.jar -C '/home/cloudera/Desktop/HW-3/PageRank/' . 

Make sure that you are into the folder in which you want to create the jar file (for me its the PageRank folder), after the above command is executed the .jar file will be created for us.

3. Once the jar file is create we need to run this jar file on Hadoop and get the output of our java file. For running the jar file use the below command:
 
[cloudera@quickstart PageRank]$ hadoop jar '/home/cloudera/Desktop/HW-3/PageRank/Driver.jar' Driver /user/cloudera/HW-3/wiki-micro.txt /user/cloudera/HW-3/output

For the above command the path after the 'jar' is the path of the .jar file for the corresponding class, then comes the class name for the same .java file then is the input directory which will give the input to the file on which we are running our pagerank code, after that is the ouput directory which will contain the output of the MapReduce job.

4. Once the .jar file is run using the above command the output will be genrated in the output directory. To view the output you can give the below command:
	
[cloudera@quickstart PageRank]	$ hadoop dfs -cat /user/cloudera/HW-3/output/finalpagerank10/p*

In the above command the last argument is the path for the output directory. Since the output file which is generated has the name part-r-00000, hence p* is used to get the output of our .java file. This will display the output on the terminal.

5. Now to save the output there are two options as described below:

	a. Go to the output directory in your browser and download the file.
	b. In the terminal you can give the following command: hadoop fs -getmerge /user/cloudera/HW-3/output/finalpagerank10/p* PageRank.out 
	 
This command will create the output file in the same folder as the user is currently in so, its better to be in the directory in which you want your output file to be saved.

