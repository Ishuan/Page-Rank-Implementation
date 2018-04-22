/* Ishan Agarwal email: iagarwa1@uncc.edu */


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class LinkGraph {

	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
		private final static IntWritable one  = new IntWritable( 1);
		private Text count  = new Text("NumberOfPages");

		Pattern linkPat = Pattern .compile("\\[\\[.*?]\\]");

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String title = lineText.toString();					//To conver the input of mapper from text to string

			Pattern p = Pattern.compile("<title>(.+?)</title>");    //To extract the page from the input wiki which is present between the <title></title> tags.
			Matcher m = p.matcher(title);          //The Matcher that will do the pattern matching on the input wiki.
			List<String> matches = new ArrayList<String>();   //An array list to store the pages.
			while (m.find()) {
				matches.add(m.group(1));				//adding a page to the list.
			}

			String line  = lineText.toString();      //To convert input of mapper from text to string.
			Matcher m1 = linkPat.matcher(line);      // extract outgoing links
			int i=0;
			List<String> outlinks = new ArrayList<String>();   //Links to store all the outlinks of the page.
			while(m1.find()) { // loop on each outgoing link
				String url = m1.group().replace("[[", "").replace("]]", "");// drop the brackets and any nested ones if any
				if(!url.isEmpty()){
					outlinks.add(i, url);     //Adding the oullinks to the list outlinks
					i++;              //incrementing the index of the list.
				}
			}
			String out_link="";        //Initializing the empty string as it will contain all the outlinks later which will be seperated by the delimiters.
			int flag=0;                //Using a flag variable which will help us in adding delimiters to the outlink string
			for (String s: outlinks){    

				if(flag==0){				//if flag=0 no delimiter will be added this is for the first outlink which will be added so that it doesn't have any delimiters before it.
					out_link +=s;
				}else{
					out_link +="@@@@@"+s;      //Adding delimiters before the outlinks 
				}
				flag++;    //Incrementing flag after 1st iteration which enable to add delimiter in between the outlinks

				outlinks=new ArrayList<String>();    //Re-initailizing outlinks list so that it can be used later for new page and its outlinks
			}


			for(String k:matches){    //Iterating over each page
 
				context.write(new Text(k), new Text(out_link));  //Emitting output of the mapper which will be of the form page outlink1@@@@@outlink2

			}
		}
	}

	public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)   
				throws IOException,  InterruptedException {
			String s= word.toString();       //Converting input of reducer to string
			String q="";           			 //Initializing the variable which will be used to store outlinks
			Double no_of_links = context.getConfiguration().getDouble("no_of_outlink", 0.0);   //Getting the toal number of outlinks to a page to calculate the initial page rank.
			double int_pr;
			for ( Text count  : counts) {
				q=count.toString();    //Storing the outlinks to the variable after converting them to string
				}

			int_pr = 1/no_of_links;    //Calculating the initial page rank of each page
			context.write(new Text(s), new Text(q+String.valueOf("#####"+int_pr)));   //Emitting the output of reducer which will be of the form Page Outlink1@@@@@Outlink2#####Initial_Page_rank
		}
	}
}
