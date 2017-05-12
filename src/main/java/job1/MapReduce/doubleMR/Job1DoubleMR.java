package job1.MapReduce.doubleMR;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import job1.MapReduce.basic.ProductWritable;


public class Job1DoubleMR {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job1 = new Job(new Configuration(), "Job1");
		job1.setJarByClass(Job1DoubleMR.class);
		job1.setMapperClass(Job1Mapper.class);
		job1.setReducerClass(Job1Reducer.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.waitForCompletion(true);
		
		Job job2 = new Job(new Configuration(),"Job2");
		job2.setJarByClass(Job1DoubleMR.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.setMapperClass(Job2Mapper.class);
		job2.setReducerClass(Job2Reducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(ProductWritable.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(1);
		job2.waitForCompletion(true);
		
		
	}

}
