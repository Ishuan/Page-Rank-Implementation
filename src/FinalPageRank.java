/* Ishan Agarwal email: iagarwa1@uncc.edu */

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class FinalPageRank  {

	public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String title = lineText.toString();

			String[] l = title.split("\t");      //splitting the string so that page goes in one index and outlinks and initial page rank in the other index
			String[] l2 =l[1].split("#####");    // splitting the outlinks and initial page rank in other indexes
			String[] pages={""};
			if(l2[0].contains("@@@@@")){           //condition : to check if outlinks contain the delimiter
				pages= l2[0].split("@@@@@");
				context.write(new Text(l[0]), new Text("link"+"\t"+l2[0]));		// it outputs the page and its outlinks
				for(String outlink: pages){			
					context.write(new Text(outlink), new Text(l2[1]+"\t"+pages.length));    // it outputs the the outlink alongwith its page rank and length of outlinks
				}
			}else if(l2[0].isEmpty()){      //Condition: if there are no outlinks 
				context.write(new Text(l[0]), new Text("link"+"\t"+""));     //writes page and appends link with an empty string
				context.write(new Text("nill"), new Text(l2[1]+"\t"+"nill")); //it outputs nill for no outlinks and as value sends initial page rank with the flag nill 
			}else if(pages.length==1) {    //Condition: to check if there is one outlink

				context.write(new Text(l[0]), new Text("link"+"\t"+l2[0]));  //writes page with link as identifier and the one outlink
				context.write(new Text(l2[0]), new Text(l2[1]+"\t"+1));  //writes the outlink with the page rank and one as count of outlinks
			}
		}

	}

	public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text word,  Iterable<Text > counts,  Context context)
				throws IOException,  InterruptedException {

			String srt = word.toString();
			int val=0;
			String key="";
			String temp="";
			double sum_ranks=0.0;
			double fpr=0.0;
			
				for ( Text count  : counts) {    //iterating over the values received from mapper 
				key = count.toString();
				String[] newKey= key.split("\t");
			
				if(word.toString().equals("nill")){   //checks if the word is nill suggesting no outlink and to skip it 
					break;	
				}else{							  

					if(newKey[0].equals("link")){  //if the value contains link identifier
						val=1;	
						if(newKey.length==2){  //if the length of the string array is 2 then proceed and set string temp variable to the outlinks
							temp = newKey[1];
						}
					}else{				

						sum_ranks= sum_ranks+ Double.parseDouble(newKey[0])/Double.parseDouble(newKey[1]);    //calculating the sum of the outlinks
						
					}

				}
			}

			if(val==1){
				fpr = 0.85*sum_ranks+0.15;         // calculating the final page rank
				context.write(new Text(srt),new Text(temp+"#####"+fpr));    //emitting the output of the reducer with page and final page rank
			}
		}
	}

}
