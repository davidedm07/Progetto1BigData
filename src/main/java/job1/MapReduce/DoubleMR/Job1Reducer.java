package job1.MapReduce.DoubleMR;

import java.io.IOException;
/*
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
*/
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {

	@Override
	public void reduce(Text key,Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		double sum = 0.0;
		double avg;
		int cont = 0;
		for (IntWritable i : values) { 
			sum += i.get();
			cont ++;
		}
		avg = sum/cont;
		
		context.write(new Text(key), new DoubleWritable(avg));
	}
}


