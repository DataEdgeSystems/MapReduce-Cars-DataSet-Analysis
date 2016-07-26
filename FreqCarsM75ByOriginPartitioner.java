package Project1;



import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FreqCarsM75ByOriginPartitioner extends Partitioner<Text, Text> {
		  
	    @Override
	    public int getPartition(Text key, Text val, int numPartitions) 
	    {
	    	int partition=0;
	    	switch(key.toString())
	    	{
	    		case "US":
	    		       partition=0;   
	    		       return partition;
			
	    		case "Europe":
	    		 		partition=1;
	    		 		return partition;
			
	    		case "Japan":
	    				partition=2;
	    				return partition;
			
	    		default: 
	    				partition=1; 
	    				return partition;
	    	}	    	
	    	
	    		 
	}

}
