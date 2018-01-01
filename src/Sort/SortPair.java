package average_sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

// 因為這個class implements WritableComparable interface，實作了compareTo方法，即使整個過程沒有Comparator，Hadoop一看到它，就會在進入reducer之前，自動幫你完成排序
public class SortPair implements WritableComparable{
	private Text word;
	private double average;

	public SortPair() {
		word = new Text();
		average = 0.0;
	}

	public SortPair(Text word, double average) {
		//TODO: constructor
		this.word = word;
		this.average = average;
	}

	@Override
	public void write(DataOutput out) throws IOException {			// 將object轉成byte (Serialize)
		word.write(out);
		out.writeDouble(average);
	}

	@Override
	public void readFields(DataInput in) throws IOException {		// 將byte轉成object (Deserialize)
		word.readFields(in);
		average = in.readDouble();
	}

	public Text getWord() {
		return word;
	}

	public double getAverage() {
		return average;
	}

	@Override
	public int compareTo(Object o) {

		double thisAverage = this.getAverage();
		double thatAverage = ((SortPair)o).getAverage();

		Text thisWord = this.getWord();
		Text thatWord = ((SortPair)o).getWord();

		// Compare between two objects
		// First order by average, and then sort them lexicographically in ascending order
			
		if(thisAverage > thatAverage)
			return -1;
		else if(thisAverage < thatAverage)
			return 1;
		else{
			if(str_compare(thisWord.toString(), thatWord.toString()) < 0)
				return -1;
			else
				return 1;
		}		
	
	}

	private int str_compare(String s1, String s2){
		return s1.compareTo(s2);
	}

} 
