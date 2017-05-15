package job1.MapReduce.DoubleMR;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job1Mapper extends Mapper<LongWritable, Text,Text, IntWritable>{

	private Text productID;
	private IntWritable score;
	private Text mapKey;


	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {

		String line = value.toString();

		if (!line.startsWith("id") || !line.equals("") || line != null) {
			String[] splitLine = line.split("\t");
			this.productID = new Text(splitLine[1]);
			this.score = new IntWritable(Integer.parseInt(splitLine[6]));
			Date time = new Date(Long.parseLong(splitLine[7])*1000);
			Calendar cal = Calendar.getInstance();
			cal.setTime(time);
			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH) + 1;
			String sMonth = "";
			if (month<10)
				sMonth = "0" + month;
			else 
				sMonth = "" + month;
				
			this.mapKey = new Text(year + "-" + sMonth + "-" + this.productID.toString());
			context.write(mapKey, this.score);
		}

	}
}
