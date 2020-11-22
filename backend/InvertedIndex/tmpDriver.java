import java.io.*;
import java.util.*;

public class tmpDriver {

    public static void main(String[] args) throws IOException {

        Scanner scan = new Scanner(new File("/Users/StevenMontalbano/Programs/cs1660/FinalProject/frontend/src/main/resources/srcFiles.txt"));
        Random rand = new Random();
        TreeSet<IndexDocument> occurrences = new TreeSet<>();

        PriorityQueue<IndexTerm> pq = new PriorityQueue<>();
        IndexTerm term = new IndexTerm("tmp1");

        int slashPos;
        String docName, docDir, path;
        ArrayList<IndexDocument> list = new ArrayList<>();

        while(scan.hasNextLine()) {
            path = scan.nextLine();
            slashPos = path.lastIndexOf("/");
            docDir = path.substring(0, slashPos);
            docName = path.substring(slashPos+1);
            list.add(new IndexDocument(docName, docDir, rand.nextInt(100)));
        }


        list.forEach(term::addDocument);
        list.clear();
        pq.add(term);

        term = new IndexTerm("tmp2");
        scan = new Scanner(new File("/Users/StevenMontalbano/Programs/cs1660/FinalProject/frontend/src/main/resources/srcFiles.txt"));

        while(scan.hasNextLine()) {
            path = scan.nextLine();
            slashPos = path.lastIndexOf("/");
            docDir = path.substring(0, slashPos);
            docName = path.substring(slashPos+1);
            list.add(new IndexDocument(docName, docDir, rand.nextInt(100)));
        }

        list.forEach(term::addDocument);
        list.clear();
        pq.add(term);

        FileOutputStream fOut = new FileOutputStream("./PQ.serial");
        ObjectOutputStream oOut = new ObjectOutputStream(fOut);

        oOut.write(pq);


        IndexTerm tmp;
        while(!pq.isEmpty()){
            tmp = pq.poll();
            System.out.println(tmp);
            int count = tmp.getFrequency();
            int tmpCount = 0;
            TreeSet<IndexDocument> map = tmp.getOccurrences();
            tmpCount += map.stream().mapToInt(IndexDocument::getFrequency).sum();

            System.out.println(count == tmpCount);

            System.out.println();
        }



    }
}
