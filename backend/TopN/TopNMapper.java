import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**	
 * TODO
 * 
 * Always export the local top N, even if that does not result in the final result being 100% accurate
 * 
 * Separate jar files for Top N and Search Term, but there is a way to reuse the constructed indicies so they are only computed once
 * 
 * @author Steven Montalbano
 */


public class TopNMapper extends Mapper<Object, Text, Text, LongWritable> {

    private static int N;     // TODO read this arg
    private TreeMap<String, Long> tMap; // <WORD, NUM_OCCURRENCES>
    private Set<String> stopWords;
    private Pattern regex;
    private Matcher match;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        regex = Pattern.compile("[a-zA-Z]+");
        tMap = new TreeMap<>();
        stopWords = Stream.of(
                "the", "of", "and", "a", "to", "in", "is", "you", "that", "if", "but", "or", "my", "his", "her", "he",
                "she", "i", "with", "for", "it", "this", "by", "as", "was", "had", "not", "him", "be", "at", "on", "your"
        ).collect(Collectors.toCollection(HashSet::new));

        Configuration config = context.getConfiguration();

        N = Integer.parseInt(config.get("N"));

        if (N == 0) {
            System.err.println("Must Specify an N Value");
            System.exit(1);
        }

    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String word = null;

        match = regex.matcher(line);

        while(match.find()) {
            word = match.group().toLowerCase();

            if (stopWords.contains(word) || word.length() == 1)  // Only add this word if it's not a stop word or
                continue;                                        // if len is 1 because Shakespeare adds 'd to end of words and that gets chopped off by the regex

            if (tMap.containsKey(word))     // Check if the word has already been seen. If so, increment its existing count,
                tMap.put(word, tMap.get(word) + 1);
            else                                // else add count = 1 to the Map
                tMap.put(word, 1L);

        }
    }

    private static TreeMap<Long, String> invertMap(TreeMap<String, Long> map) {
        TreeMap<Long, String> swapped = new TreeMap<>(Collections.reverseOrder());

        for (Map.Entry<String, Long> e: map.entrySet())
            swapped.put(e.getValue(), e.getKey());

        return swapped;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        TreeMap<Long, String> swapped = invertMap(tMap);

        int i = 1;
        for (Map.Entry<Long, String> e: swapped.entrySet()) {
            if (i++ > N)
                break;
            context.write(new Text(e.getValue()), new LongWritable(e.getKey()));
        }

    }

}
