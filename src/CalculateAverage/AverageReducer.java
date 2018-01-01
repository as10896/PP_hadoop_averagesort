package average_sort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, SumCountPair, Text, Text> {
	
    public void reduce(Text key, Iterable<SumCountPair> values, Context context) throws IOException, InterruptedException {
		// output the word and average
				
		double sum=0, count=0;
		for (SumCountPair val: values) {
			sum += val.getSum();
			count += val.getCount();
		}

		double avg = sum / count;
		Text V = new Text(String.valueOf(avg));
		
		context.write(key, V);

	}
}
