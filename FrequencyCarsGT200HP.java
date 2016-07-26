package Project1;

import java.io.IOException;
import java.util.Set;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;

import Project1.DoublePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FrequencyCarsGT200HP
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "FrequencyCarsGT200HP");
	        job.setJarByClass(StableDirector.class); 
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setMapperClass(FreqCarsGT200HPMapper.class);
	 	    	    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		                	    
 		    job.setNumReduceTasks(1);
 		    
	 	    job.setReducerClass(FreqCarsGT200HPReducer.class);
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
            FileOutputFormat.setOutputPath(job, new Path(args[1]));    		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	 	     	    	 	    
	}
}

class FreqCarsGT200HPMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
	
	
	
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
	  
	  		 
	 if(!line[0].equals("Car") && !line[4].equals("Horsepower") && 
	     !line[0].equals("STRING") && !line[4].equals("DOUBLE") &&
	     !line[0].equals("") && !line[4].equals("")) 
	    {
		   String Name_Of_Car = line[0].trim(); 
		   Double HP_Of_Car = Double.parseDouble(trimPeriod(line[4].trim()));
		   
	       if(HP_Of_Car>200.0) 
		   {
			   context.write(new Text(Name_Of_Car),new IntWritable(1));
		   }	   
   
        }
  }	

  public static String trimPeriod1 (String value)
	  {
		  if(value == null)
			return value;

          if(value.endsWith("."))
		  {
			  value=value.substring(0,value.length()-1);
		  }		
		return value;		  
	  }
				
  
} 

class FreqCarsGT200HPReducer extends Reducer <Text,IntWritable,Text,IntWritable> 
{
		
		
    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
    {        
    
	    int sum=0;	 
       for (IntWritable val:values)
       {      	
    	 
	       sum+=val.get();
			 
	   }
	   context.write(key,new IntWritable(sum));
       
  }   	      
        
}
	   
	
    