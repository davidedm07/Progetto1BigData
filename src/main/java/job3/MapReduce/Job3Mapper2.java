package job3.MapReduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

@SuppressWarnings("unused")
public class Job3Mapper2 extends Mapper<LongWritable, Text,Text, Text> {

	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] splitLine  = line.split("\t");
		String couple = splitLine[0];
		String productId = splitLine[1];
		context.write(new Text(couple), new Text(productId));
		
	}

}
