package job2.MapReduce.topTenVotes;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopTenMapper extends Mapper<LongWritable, Text,Text, ProductWritable> {
	
	private Text userId;
	private ProductWritable product;
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		if (!line.equals("") || line != null) {
			String[] splittedLine = line.split("\t");
			this.userId = new Text(splittedLine[2]);
			this.product = new ProductWritable(new Text(splittedLine[1]), 
					new DoubleWritable(Double.parseDouble(splittedLine[6])));
			context.write(userId, product);
		}
	}
}
