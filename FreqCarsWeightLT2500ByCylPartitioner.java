package Project1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FreqCarsWeightLT2500ByCylPartitioner extends Partitioner<IntWritable, IntWritable>
{
	@Override
    public int getPartition(IntWritable key, IntWritable val, int numPartitions) 
    {
    	int partition=0;
    	switch(key.get())
    	{
    		case 3:
    		       partition=0;   
    		       return partition;
		
    		case 4:
    		 		partition=1;
    		 		return partition;
		
    		case 6:
    				partition=2;
    				return partition;
		
    		default: 
    				partition=1; 
    				return partition;
    	}	    	
    	
    		 
}

}

