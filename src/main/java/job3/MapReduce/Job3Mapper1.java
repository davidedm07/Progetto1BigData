package job3.MapReduce;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class Job3Mapper1  extends Mapper<LongWritable, Text,Text, Text>{

	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] splitLine  = line.split("\t");
		String productId = splitLine[1];
		String userId = splitLine[2];
		int score = Integer.parseInt(splitLine[6]);
		if (score>=4)
			context.write(new Text(productId), new Text(userId));
		
	}
}



