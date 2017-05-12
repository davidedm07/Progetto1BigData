package job2.MapReduce.topTenVotes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopTenReducer extends Reducer<Text, ProductWritable, Text, Text>{

	@Override
    protected void reduce(Text key, Iterable<ProductWritable> values, Context context)
            throws IOException, InterruptedException {
       
        Map<String, Integer> prod2score = new TreeMap<String, Integer>();
       
        for (ProductWritable product: values){
        	prod2score.put(product.getProductId().toString(), product.getScore().get());
        }
       
        Map<String, Integer> sortedMap = sortByValues(prod2score);
       
        int counter = 0;
        ArrayList<String> result = new ArrayList<String>();
        for (String i : sortedMap.keySet()) {
            if(counter < 10) {
            	result.add("Product Id: " + i + " Score: " + sortedMap.get(i));
            	counter ++;
            } 
            else { 
            	break; }    
        }
        context.write(key, new Text(result.toString()));
       
    }
   
   
   
   
   
    @SuppressWarnings("rawtypes")
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());
 
        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {
 
            @SuppressWarnings("unchecked")
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
 
        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();
 
        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }
 
        return sortedMap;
    }

}
