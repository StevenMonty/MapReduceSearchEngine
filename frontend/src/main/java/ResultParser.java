import javax.swing.*;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResultParser extends SwingWorker<LinkedHashMap<String, IndexTerm>, Void> {

    LinkedHashMap<String, IndexTerm> hmap = new LinkedHashMap<>();

    @Override
    protected LinkedHashMap<String, IndexTerm> doInBackground() throws Exception {

        System.out.println("SwingWorker doInBackground called");

        try {

            Scanner scan = new Scanner(new File("/Users/StevenMontalbano/Programs/cs1660/FinalProject/part-r-00000"));

            String line;

            PriorityQueue<IndexTerm> pq = new PriorityQueue<>();
            IndexTerm term;
            IndexDocument doc;

            String word, ID, dir, name = null, freq = null;

            Pattern regex = Pattern.compile("\\{([^}]+)\\}");
            Matcher match;
            String[] pieces;

            ArrayList<IndexDocument> docList;


            while (scan.hasNextLine()) {
                line = scan.nextLine().replaceAll("'", "");
                word = line.split(":", 2)[0];
                match = regex.matcher(line.split(":", 2)[1]);
                docList = new ArrayList<>();

                while (match.find()) {
                    pieces = match.group().replaceAll("\\{*\\}*", "").split("[,:]+");
                    ID = pieces[1];
                    dir = pieces[3];
                    name = pieces[5];
                    freq = pieces[7];
                    docList.add(new IndexDocument(name, dir, freq, ID));    // IndexDocument(String docName, String docDir, int frequency, int docID)
                }

                term = new IndexTerm(word);
                term.addDocuments(docList);
                pq.add(term);
            }

            scan.close();

            while (!pq.isEmpty()) {

                term = pq.poll();
//                System.out.println(term.getTerm() + "(tot: " + term.getFrequency() + ") :" + term.getOccurrences().toString());
                hmap.put(term.getTerm(), term);
            }

//            System.out.println();
//            System.out.println();
//
//            Iterator<Map.Entry<String, IndexTerm>> it = hmap.entrySet().iterator();
//            Map.Entry<String, IndexTerm> e;
//            while (it.hasNext()) {
//                e = it.next();
////                System.out.println(e.getKey() + "(tot: " + e.getValue().getFrequency() + ") :" + e.getValue().getOccurrences().toString());
//            }

//            return true;

        } catch (Exception e) {
            e.printStackTrace();
//            return false;
        }

        System.out.println("SwingWorker doInBackground done.");

        return hmap;
    }

    @Override
    public void done(){

    }

}
