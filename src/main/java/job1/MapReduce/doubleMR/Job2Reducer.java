package job1.MapReduce.doubleMR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import job1.MapReduce.basic.ProductWritable;

public class Job2Reducer extends  Reducer<Text, ProductWritable, Text, Text>{

	@Override
	public void reduce(Text key,Iterable<ProductWritable> values, Context context) 
			throws IOException, InterruptedException {

		ArrayList<ProductWritable> list =new ArrayList<ProductWritable>();
		for (ProductWritable p : values) {
			String productId = p.getProductID().toString();
			String averageScore = p.getAverageScore().toString();
			ProductWritable x = new ProductWritable(new Text(productId),new DoubleWritable(Double.parseDouble(averageScore)));
			list.add(x);
		}
		
		Collections.sort(list);
		Collections.reverse(list);
		List<ProductWritable> top5;
		if (list.size()>5)
			top5= new ArrayList<ProductWritable>(list.subList(0, 5));
		else 
			top5 = list;
		context.write(key,new Text(top5.toString()));

	}

}
