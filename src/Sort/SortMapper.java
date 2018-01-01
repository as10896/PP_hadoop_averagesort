package average_sort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SortMapper extends Mapper<Text, Text, SortPair, NullWritable> {
	
	// private static final Log LOG = LogFactory.getLog(SortMapper.class);

	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		SortPair K = new SortPair(key, Double.parseDouble(value.toString()));

		// LOG.info("(Mapper)key: " + K.getWord().toString());
		// LOG.info("(Mapper)avg: " + Double.toString(K.getAverage()) + "\n");
		
		context.write(K, NullWritable.get());
	}
	
}
