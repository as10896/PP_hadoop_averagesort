package average_sort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SortReducer extends Reducer<SortPair, NullWritable, Text, Text> {

    // private static final Log LOG = LogFactory.getLog(SortMapper.class);
    
    public void reduce(SortPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // output the word and average
        
        // LOG.info("(Reducer)key: " + key.getWord().toString());
		// LOG.info("(Reducer)avg: " + Double.toString(key.getAverage()) + "\n");    

        Text K = key.getWord();
        Text V = new Text(Double.toString(key.getAverage()));

        context.write(K, V);

    }
}