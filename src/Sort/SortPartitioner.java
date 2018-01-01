package average_sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.NullWritable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SortPartitioner extends Partitioner<SortPair, NullWritable> {
	
	private double maxValue = 11.05;
	private double minValue = 9.0;

	// private static final Log LOG = LogFactory.getLog(SortMapper.class);

	@Override
	public int getPartition(SortPair key, NullWritable value, int numReduceTasks) {
		
		int num;
		
		// customize which <K ,V> will go to which reducer
		// Based on defined min/max value and numReduceTasks
		double offset = (maxValue - minValue) / numReduceTasks;
		num = (int) Math.floor((key.getAverage() - minValue) / offset);
		
		// LOG.info("(Partitioner)num: " + Integer.toString(num));
		// LOG.info("(Partitioner)numReduceTasks: " + Integer.toString(numReduceTasks));
		// LOG.info("(Partitioner)return: " + Integer.toString(numReduceTasks - 1 - num) + "\n");

		if(num == numReduceTasks)
			return 0;
		else
			return (numReduceTasks - 1 - num);
	}
}
