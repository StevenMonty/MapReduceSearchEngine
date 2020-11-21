import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap<String, Integer> map;
    private StringBuilder res;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        map  = new HashMap<String, Integer>();
        res = new StringBuilder();

        String val;
        for(Text value: values){
            val = value.toString();
            System.out.println("Reduce, Key: " + key + " vals: " + val);
            map.merge(val, 1, Integer::sum);
        }

        for (Map.Entry<String, Integer> entry: map.entrySet())
            res.append(entry.getKey() + "/" + entry.getValue() + ";");

        System.out.println("Reduce writing to context, key: " + key + " val: " + res.toString());
        context.write(key, new Text(res.toString()));

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Get the first N elements from the tMap, since they are stored in ascending order on the key which is the
        // number of occurrences for that value (the word)
//        TreeMap<Long, String> result = tMap2.entrySet().stream().limit(N).collect(
//                TreeMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);
//
//        String s;
//        long l;
//        // Iterate through the results TreeMap just created and write the results to the context.
//        for (Map.Entry<Long, String> entry: result.entrySet()){
//
//            s = entry.getValue();
//            l = entry.getKey();
//
//            context.write(new Text(l), new Text(s));
//        }

    }
}
