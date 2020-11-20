import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

    private Text word;
    private IntWritable one;
    private FileSplit split;
    private Integer docID;
    private StringTokenizer tokens;
    private Set<String> stopWords;
    private Pattern regex;
    private Matcher match;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        regex = Pattern.compile("[a-zA-Z]+");
        stopWords = Stream.of(
                "the", "of", "and", "a", "to", "in", "is", "you", "that", "if", "but", "or", "my", "his", "her", "he",
                "she", "i", "with", "for", "it", "this", "by", "as", "was", "had", "not", "him", "be", "at", "on", "your"
        ).collect(Collectors.toCollection(HashSet::new));

        word = new Text();
        one = new IntWritable(1);
        split = (FileSplit)context.getInputSplit();
        docID = split.getPath().getName().hashCode();   // Generate document ID to be the hash of the file name
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        tokens = new StringTokenizer(value.toString(), "\n"); // TODO need diff delim

        String val = value.toString();

        // Check if the word has already been seen. If so, increment its existing count, else add count = 1 to the Map
//        if (tMap.containsKey(val))
//            tMap.put(val, tMap.get(val)+1);
//        else
//            tMap.put(val, 1L);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

//        String s;
//        long l;
//        for (Map.Entry<String, Long> entry: tMap.entrySet()){
//            s = entry.getKey();
//            l = entry.getValue();
//
//            context.write(new Text(s), new LongWritable(l));
//        }

    }

}
