import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;


public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

    private HashMap<String, Integer> files; // <FILE_PATH, OCCURRENCES_IN_THIS_FILE>
    private StringBuilder res;
    private IndexTerm term;
//    private IndexDocument doc;
    private PriorityQueue<IndexTerm> pq;
    private HashMap<String, IndexTerm> map;
    int i;
    private boolean DEBUG_MODE;

    int slashPos;
    String docName, docDir, path;
    ArrayList<IndexDocument> docList;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        map = new HashMap<>();
//        pq = new PriorityQueue<>();
        i = 0;
        DEBUG_MODE = context.getConfiguration().getBoolean("DEBUG_MODE", false);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        files = new HashMap<String, Integer>();
        res = new StringBuilder();

        String val;
        for(Text value: values){
            val = value.toString();
            files.merge(val, 1, Integer::sum);
        }

        if (map.containsKey(key.toString()))
            term = map.get(key.toString());
        else
            term = new IndexTerm(key.toString());


        docList = new ArrayList<>();
        for (Map.Entry<String, Integer> entry: files.entrySet()){
            slashPos = entry.getKey().lastIndexOf("/");
            docDir = entry.getKey().substring(0, slashPos);
            docName = entry.getKey().substring(slashPos+1);
            docList.add(new IndexDocument(docName, docDir, entry.getValue()));
        }

        term.addDocuments(docList);

        map.put(key.toString(), term);

//        if (i++ % 100 == 0 && DEBUG_MODE)
//            System.out.println("Reduce writing to context, key: " + key + " val: " + res.toString());

//        context.write(key, new Text(res.toString()));

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("In Reducer Cleanup:");
        pq = new PriorityQueue<>(map.values());
        Iterator<IndexTerm> it = pq.iterator();

        while (it.hasNext()){
            term = it.next();
            context.write(new Text(term.getTerm() + " (tot:" + term.getFrequency() + ")"), new Text(term.getOccurrences().toString()));

        }

    }
}
