package job3.MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import com.google.common.collect.Lists;


@SuppressWarnings("unused")
public class Job3Reducer2 extends  Reducer<Text,Text,Text,Text> {
	
	private HashMap<String,String> couplesListOfProducts = new HashMap<>();

	@Override
	public void reduce(Text key,Iterable<Text> values,Context context) 
			throws IOException, InterruptedException {
		
		ArrayList<Text> productsForCouple = Lists.newArrayList(values);
		if (productsForCouple.size()>=3)
			//couplesListOfProducts.put(key.toString(),productsForCouple.toString());
			context.write(key, new Text(productsForCouple.toString()));

	}
}
