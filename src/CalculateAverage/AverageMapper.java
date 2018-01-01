package average_sort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<Text, Text, Text, SumCountPair> {	
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		// key for word, value for count
		// context.write(K,V)
		
		int sum = Integer.parseInt(value.toString());
		SumCountPair V = new SumCountPair(sum, 1);
		
		context.write(key, V);
	}
}
