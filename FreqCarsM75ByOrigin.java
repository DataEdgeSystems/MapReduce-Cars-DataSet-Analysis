package Project1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FreqCarsM75ByOrigin
{
	public static void main(String[] args) throws Exception	{
		
			Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "FreqCarsM75ByOrigin");
	        job.setJarByClass(FreqCarsM75ByOrigin.class); 
	        job.setInputFormatClass(TextInputFormat.class);
	        job.setMapperClass(FreqCarsM75ByOriginMapper.class);
	 	    	    
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		                	    
 		    job.setNumReduceTasks(3);
 		    job.setPartitionerClass(FreqCarsM75ByOriginPartitioner.class);
 		    
	 	    job.setReducerClass(FreqCarsM75ByOriginReducer.class);
	 	    
	 	    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
			// File Input Format
		    FileInputFormat.addInputPath(job, new Path(args[0]));		    
		    
		    // File Output Format
            FileOutputFormat.setOutputPath(job, new Path(args[1]));    		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	 	     	    	 	    
	}
}

class FreqCarsM75ByOriginMapper extends Mapper<LongWritable, Text, Text, Text>
{
	
	
	
	  @Override  
  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
  {
	 String delim1 = ";";
	 String[] line= value.toString().split(delim1);
	  
	  		 
	 if(!line[7].equals("Model") && !line[8].equals("Origin") && !line[0].equals("Car") &&
	     !line[7].equals("INT") && !line[8].equals("CAT") && !line[0].equals("STRING") &&
	     !line[7].equals("") && !line[8].equals("") && !line[0].equals(""))
		 
	    {
		   int Model_Of_Car = Integer.parseInt(line[7].trim());		   
		   
		   if(Model_Of_Car==75)
		   {
			   String Origin_Of_car = line[8].trim();
			   String Name_Of_Car = line[0].trim();
			   context.write(new Text(Origin_Of_car),new Text(Name_Of_Car));
		   }	   
   
        }
  }	

  
} 

class FreqCarsM75ByOriginReducer extends Reducer <Text,Text,Text,Text> 
{
		
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
    {        
    
	    int sum=0;	
	    String sl_no="";
        context.write(new Text("CARS OF MODEL 75 ORIGINATED FROM"), new Text(key));
		 
       for (Text val:values)
       {      	   
    	   String Nm_Of_Car = val.toString();
    	   sum+=1;		 
    	   sl_no = sum+"";
		   context.write(new Text(Nm_Of_Car), new Text(sl_no));
		   
	   }
	   
       
  }   	
    
        
}
	   
	
    