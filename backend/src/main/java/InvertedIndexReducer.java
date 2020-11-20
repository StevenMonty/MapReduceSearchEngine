import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class InvertedIndexReducer extends Reducer<Text, LongWritable, LongWritable, Text> {

    private final static int N = 5;// TODO read arg
    private TreeMap<Long, String> tMap2; // <NUM_OCCURRENCES, WORD>


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        tMap2 = new TreeMap<>();
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        String word = key.toString();

        long l = 0;
        for(LongWritable val: values){
            l += val.get();
        }

        tMap2.put(l, word);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        // Get the first N elements from the tMap, since they are stored in ascending order on the key which is the
        // number of occurrences for that value (the word)
        TreeMap<Long, String> result = tMap2.entrySet().stream().limit(N).collect(
                TreeMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

        String s;
        long l;
        // Iterate through the results TreeMap just created and write the results to the context.
        for (Map.Entry<Long, String> entry: result.entrySet()){

            s = entry.getValue();
            l = entry.getKey();

            context.write(new LongWritable(l), new Text(s));
        }

    }
}
