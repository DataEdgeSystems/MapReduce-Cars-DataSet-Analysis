package Project1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FreqCarsWeightLT2500ByCyl
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "FreqCarsWeightLT2500ByCyl");
	        job.setJarByClass(FreqCarsWeightLT2500ByCyl.class); 
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setMapperClass(FreqCarsWeightLT2500ByCylMapper.class);
	 	    	    
		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(IntWritable.class);
		                	    
 		    job.setNumReduceTasks(3);
 		    job.setPartitionerClass(FreqCarsWeightLT2500ByCylPartitioner.class);
 		    
	 	    job.setReducerClass(FreqCarsWeightLT2500ByCylReducer.class);
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
            FileOutputFormat.setOutputPath(job, new Path(args[1]));    		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	 	     	    	 	    
	}
}

class FreqCarsWeightLT2500ByCylMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>
{
	 @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
	  
	  		 
	 if(!line[2].equals("Cylinders") && !line[5].equals("Weight") &&  
	     !line[2].equals("INT") && !line[5].equals("DOUBLE") && 
	     !line[2].equals("") && !line[5].equals("")) 		 
	    {
			
			int Cylinders=Integer.parseInt(line[2].trim());
			Double Weight_Of_Car = Double.parseDouble(trimPeriod(line[5].trim()));
			int ret = Double.compare(Weight_Of_Car,2500.00);
			
			if (ret<0)
			{
			   context.write(new IntWritable(Cylinders),new IntWritable(1));
		    }	   
   
        }
  }

    public static String trimPeriod (String value)
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

class FreqCarsWeightLT2500ByCylReducer extends Reducer <IntWritable,IntWritable,Text,Text> 
{
		
    public void reduce(IntWritable key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
    {        
    
	    int Num_Of_Cars=0;	
		String header1 = "NUMBER OF CARS WITH "+key+" CYLINDERS";
		String header2 = "";
        context.write(new Text(header1), new Text(header2)); //Used header2 as a filler here 
		 
       for (IntWritable val:values)
       {      	   
    	    Num_Of_Cars+= val.get();
		   
		   	   
	   }
	   String count="\t\t"+Num_Of_Cars;
	   context.write(new Text(count), new Text(header2)); //Used header2 as filler here 
	   
	   
       
  }   	
    
        
}
	   
	
    