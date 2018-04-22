/* Ishan Agarwal email: iagarwa1@uncc.edu */

/* This class is our main class and running this class will run all the other classes and will give the final page rank. */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class Driver extends Configured  implements Tool {
	private static final Logger LOG = Logger .getLogger( Driver.class);

	public static void main( String[] args) throws  Exception {
		int res  = ToolRunner .run( new Driver(), args);
		System .exit(res);
	}

	@SuppressWarnings("deprecation")
	public int run( String[] args) throws  Exception {

		//Below is the starting of Job-1 which will run the TotalPages.java class and will be used to calculate the total number of pages in the input wiki this job is very much similar to the wordcount job

		Job job1  = Job .getInstance(getConf(), " TotalNumberofPages ");
		job1.setJarByClass( this .getClass());

		FileInputFormat.addInputPaths(job1,  args[0]);
		FileOutputFormat.setOutputPath(job1,  new Path(args[ 1]));
		job1.setMapperClass( TotalPages.Map .class);
		job1.setReducerClass( TotalPages.Reduce .class);
		job1.setMapOutputKeyClass( Text .class);
		job1.setMapOutputValueClass( IntWritable .class);
		job1.setOutputKeyClass( Text .class);
		job1.setOutputValueClass( IntWritable .class);

		job1.waitForCompletion( true);

		//Below is the starting of Job-2 which will run the LinkGraph.java class and will be used to generated the link graph of the input wiki and will assign iniital page rank to each page which will be 1/N. The Link Graph is of the following form Page Outlink1@@@@@Outlink2#####InitialPageRank

		Configuration config= new Configuration();
		try{

			FileSystem fs = FileSystem.get(config);      //Here we're using the File System to read the output of the TotalPages.java file to get the total number of pages and assign initial page rank
			Path input_path= new Path(args[1]+"/part-r-00000");     //The input path of the TotalPages.java output file


			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(input_path)));
			String line;
			double no_of_outlink=0.0;        //We are taking the variable to get the total number of outlinks of the page which will we used in the reducer to calculate initial page rank.

			while((line = br.readLine())!= null){     // This will read each line of the input and will terminate when there are no more lines to read
				String [] l = line.split("\t");
				no_of_outlink=Double.parseDouble(l[1]);  //Getting the total number of outlinks to a page.
				break;
			}
			fs.delete(new Path(args[1]),true);          //deleting the output of the totalnumber of pages file since we only want the final page rank.
			br.close();
			config.setDouble("no_of_outlink", no_of_outlink);   //Creating a configuration which will be later used by the reducer of LinkGraph.java to get the total number of outlinks of a page.
		}catch(ArrayIndexOutOfBoundsException e){
			System.out.println(e);
		}

		Job job2  = Job .getInstance(config, " linkgraph ");
		job2.setJarByClass( this .getClass());

		FileInputFormat.addInputPaths(job2,  args[0]);
		FileOutputFormat.setOutputPath(job2,  new Path(args[ 1]+"/finalpagerank0"));
		job2.setMapperClass( LinkGraph.Map .class);
		job2.setReducerClass( LinkGraph.Reduce .class);
		job2.setOutputKeyClass( Text .class);
		job2.setOutputValueClass( Text .class);


		job2.waitForCompletion(true);

		//Below is the starting of Job-3 which will run the FinalPageRank.java class and will be used to calculate the final page rank of each page in input wiki.

		for(int i=0; i<10; i++){      									//Initiating a loop which will run for 10 times to converge the page rank values.
			Job job3  = Job .getInstance(getConf(), " FinalPageRank ");
			Configuration conf = new Configuration();
			String input= args[1]+"/finalpagerank"+i;			//Setting the path of the input folder which will be used by each iteration to get the previous page ranks
			String output= args[1]+"/finalpagerank"+(i+1);		//Setting up the path of the output folder which will hold the final page rank after each iteration
			FileSystem fs = FileSystem.get(conf);

			job3.setJarByClass( this .getClass());

			FileInputFormat.addInputPaths(job3, input);
			FileOutputFormat.setOutputPath(job3, new Path(output));
			job3.setMapperClass( FinalPageRank.Map .class);
			job3.setReducerClass( FinalPageRank.Reduce .class);

			job3.setOutputKeyClass( Text .class);
			job3.setOutputValueClass( Text .class);
			job3.waitForCompletion(true);

			fs.delete(new Path(input),true);    				//Deleting the output folder after each iteration and this will keep the result of only the last iteration
		}

		//Below is the starting of Job-4 which will run the FinalSortedRank.java class which will be used to sort the final page ranks

		String input_outpath= args[1]+"/finalpagerank10/part-r-00000";   //setting the input path to read the input that needs to be sorted
		String outpath= args[1]+"/sorted";								//setting the output path to store the output after sorting

		Job job4  = Job .getInstance(getConf(), " FinalSortedRank ");
		Configuration config2= new Configuration();
		job4.setJarByClass( this .getClass());
		FileSystem fs1= FileSystem.get(config2);    //using FileSystem to manage the sorted output files.

		FileInputFormat.addInputPaths(job4,  input_outpath);
		FileOutputFormat.setOutputPath(job4,  new Path(outpath));
		job4.setMapperClass( FinalSortedRank.Map .class);
		job4.setReducerClass( FinalSortedRank.Reduce .class);
		job4.setMapOutputKeyClass( DoubleWritable .class);
		job4.setMapOutputValueClass( Text .class);
		job4.setOutputKeyClass( Text .class);
		job4.setOutputValueClass( DoubleWritable .class);
		Path a= new Path(args[1]+"/finalpagerank10");
		job4.setSortComparatorClass(sorting_comp.class);    //Comparator to be used by this job to sort the output

		job4.waitForCompletion( true);
		fs1.delete(a,true);    //deleting the folder of the last iteration of the FinalPageRank.java class
		return 0;
	}
}
//Below is the code for the sorting that we are doing to sort the final page ranks. 


class sorting_comp extends WritableComparator {     //Sorting: Comparator Class 

	protected sorting_comp(){
		super (DoubleWritable.class, true);         // Setting up the constructor of comparator to accept the values of DoubleWritable type
	}

	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable c1, @SuppressWarnings("rawtypes") WritableComparable c2){

		DoubleWritable val1 = (DoubleWritable) c1;      //This will store the value of one of the arguments and will use them later for comparison.
		DoubleWritable val2 = (DoubleWritable) c2;		//This will store the other value of one of the arguments and will use them later for comparison.

		int sorted_res = val1.compareTo(val2);   //This will store the result of the comparison of the two arguments
		if(sorted_res == 0){					
			sorted_res = val1.compareTo(val2);
		}
		return sorted_res*-1;   //To sort the result in decreasing value


	}
}

