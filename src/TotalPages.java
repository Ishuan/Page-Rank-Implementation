/* Ishan Agarwal email: iagarwa1@uncc.edu */


import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TotalPages{


	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
		private final static IntWritable one  = new IntWritable( 1);


		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();  //To conver the input of mapper  from text to string


			if(!line.isEmpty()){
				context.write(new Text("count"),one);     //Emitting the output of the Mapper with the page and its count.
			}
		}
	}

	public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
		@Override 
		public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
				throws IOException,  InterruptedException {
			int sum  = 0;							//The sum variable will be used to store and increment the total number of pages.
			for ( IntWritable count  : counts) {
				sum  += count.get();
			}

			context.write(word, new IntWritable(sum));   //Emitting the output of reducer which will be the total number of pages.
		}
	}


}
