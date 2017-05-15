package job1.MapReduce.DoubleMR;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;

import job1.MapReduce.basic.ProductWritable;

public class Job2Mapper extends Mapper<LongWritable, Text,Text, ProductWritable> {
	
	public void map(LongWritable key,Text value, Context context) 
	throws IOException, InterruptedException  {
		String[] splitLine1 = value.toString().split("\t");
		String[] splitLine2 = splitLine1[0].toString().split("-");
		String newKey = splitLine2[0] + "-" + splitLine2[1];
		DoubleWritable d = new DoubleWritable(Double.parseDouble(splitLine1[1]));
		ProductWritable p = new ProductWritable(new Text(splitLine2[2]),d);
		context.write(new Text(newKey), p);		
	}
	

}
