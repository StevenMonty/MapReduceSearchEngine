import java.io.*;
import java.util.*;

public class tmpDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException {

        Scanner scan = new Scanner(new File("/Users/StevenMontalbano/Programs/cs1660/FinalProject/frontend/src/main/resources/srcFiles.txt"));
        Random rand = new Random();
        TreeSet<IndexDocument> occurrences = new TreeSet<>();
//
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

//        pq.contains(new IndexTerm())

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



////        FileOutputStream fOut = new FileOutputStream("./PQ.serial");
//        FileInputStream fIn = new FileInputStream("./PQ.serial");
////        ObjectOutputStream oOut = new ObjectOutputStream(fIn);
//        ObjectInputStream oIn = new ObjectInputStream(fIn);
//
//        pq = (PriorityQueue<IndexTerm>) oIn.readObject();

//        oOut.close();
//        fOut.close();
//        oIn.close();
//        fIn.close();

//        System.out.println(pq);



        /*

Term{term:'tmp1', freq:2716, occurrences:[Doc{ID=230926456, docDir:'Shakespeare/comedies', docName:'comedyoferrors', freq:97}, Doc{ID=1024834754, docDir:'Shakespeare/tragedies', docName:'romeoandjuliet', freq:96}, Doc{ID=500766537, docDir:'Shakespeare/poetry', docName:'sonnets', freq:94}, Doc{ID=518112498, docDir:'Shakespeare/histories', docName:'kingjohn', freq:94}, Doc{ID=463475120, docDir:'Shakespeare/comedies', docName:'twogentlemenofverona', freq:90}, Doc{ID=630475680, docDir:'Shakespeare/histories', docName:'kinghenryviii', freq:86}, Doc{ID=1353411103, docDir:'Shakespeare/comedies', docName:'winterstale', freq:85}, Doc{ID=1049599361, docDir:'Hugo', docName:'NotreDame_De_Paris.txt', freq:85}, Doc{ID=1889421663, docDir:'Shakespeare/histories', docName:'kinghenryv', freq:83}, Doc{ID=162280924, docDir:'Shakespeare/comedies', docName:'asyoulikeit', freq:83}, Doc{ID=1764491162, docDir:'Shakespeare/poetry', docName:'various', freq:82}, Doc{ID=478568825, docDir:'Shakespeare/poetry', docName:'loverscomplaint', freq:79}, Doc{ID=382588277, docDir:'Shakespeare/tragedies', docName:'coriolanus', freq:78}, Doc{ID=1946807979, docDir:'Shakespeare/comedies', docName:'twelfthnight', freq:77}, Doc{ID=1746469475, docDir:'Shakespeare', docName:'README', freq:77}, Doc{ID=862744349, docDir:'Shakespeare/poetry', docName:'rapeoflucrece', freq:75}, Doc{ID=2073839787, docDir:'Shakespeare/tragedies', docName:'kinglear', freq:75}, Doc{ID=1864731932, docDir:'Shakespeare/comedies', docName:'tamingoftheshrew', freq:72}, Doc{ID=1479279415, docDir:'Shakespeare/tragedies', docName:'othello', freq:72}, Doc{ID=1651672775, docDir:'Shakespeare/tragedies', docName:'titusandronicus', freq:69}, Doc{ID=1462743699, docDir:'Shakespeare/tragedies', docName:'timonofathens', freq:65}, Doc{ID=1713597585, docDir:'Shakespeare/histories', docName:'1kinghenryvi', freq:64}, Doc{ID=1531259565, docDir:'Shakespeare/comedies', docName:'merrywivesofwindsor', freq:63}, Doc{ID=1211414136, docDir:'Shakespeare/comedies', docName:'periclesprinceoftyre', freq:62}, Doc{ID=519792870, docDir:'Shakespeare/comedies', docName:'allswellthatendswell', freq:61}, Doc{ID=390268403, docDir:'Shakespeare', docName:'glossary', freq:59}, Doc{ID=827479467, docDir:'Shakespeare/poetry', docName:'venusandadonis', freq:54}, Doc{ID=2054600219, docDir:'Shakespeare/comedies', docName:'cymbeline', freq:54}, Doc{ID=1713597975, docDir:'Shakespeare/histories', docName:'1kinghenryiv', freq:54}, Doc{ID=629569265, docDir:'Shakespeare/comedies', docName:'measureforemeasure', freq:52}, Doc{ID=1497810337, docDir:'Tolstoy', docName:'anna_karenhina.txt', freq:51}, Doc{ID=1553272819, docDir:'Shakespeare/tragedies', docName:'juliuscaesar', freq:46}, Doc{ID=1400842289, docDir:'Shakespeare/tragedies', docName:'hamlet', freq:46}, Doc{ID=113560190, docDir:'Shakespeare/comedies', docName:'troilusandcressida', freq:43}, Doc{ID=2106705121, docDir:'Shakespeare/comedies', docName:'tempest', freq:43}, Doc{ID=964051240, docDir:'Shakespeare/comedies', docName:'merchantofvenice', freq:41}, Doc{ID=1184641981, docDir:'Hugo', docName:'Miserables.txt', freq:38}, Doc{ID=1584514866, docDir:'Shakespeare/histories', docName:'2kinghenryvi', freq:35}, Doc{ID=1560939530, docDir:'Tolstoy', docName:'war_and_peace.txt', freq:32}, Doc{ID=264479604, docDir:'Shakespeare/comedies', docName:'muchadoaboutnothing', freq:22}, Doc{ID=1455432147, docDir:'Shakespeare/histories', docName:'3kinghenryvi', freq:22}, Doc{ID=983496988, docDir:'Shakespeare/comedies', docName:'loveslabourslost', freq:20}, Doc{ID=1767612775, docDir:'Shakespeare/comedies', docName:'midsummersnightsdream', freq:15}, Doc{ID=330062964, docDir:'Shakespeare/histories', docName:'kingrichardii', freq:12}, Doc{ID=1646763189, docDir:'Shakespeare/histories', docName:'kingrichardiii', freq:8}, Doc{ID=1584515256, docDir:'Shakespeare/histories', docName:'2kinghenryiv', freq:3}, Doc{ID=844395280, docDir:'Shakespeare/tragedies', docName:'macbeth', freq:1}, Doc{ID=745969915, docDir:'Shakespeare/tragedies', docName:'antonyandcleopatra', freq:1}]}
true

Term{term:'tmp2', freq:2222, occurrences:[Doc{ID=113560190, docDir:'Shakespeare/comedies', docName:'troilusandcressida', freq:99}, Doc{ID=1462743699, docDir:'Shakespeare/tragedies', docName:'timonofathens', freq:98}, Doc{ID=964051240, docDir:'Shakespeare/comedies', docName:'merchantofvenice', freq:95}, Doc{ID=1400842289, docDir:'Shakespeare/tragedies', docName:'hamlet', freq:95}, Doc{ID=1553272819, docDir:'Shakespeare/tragedies', docName:'juliuscaesar', freq:91}, Doc{ID=519792870, docDir:'Shakespeare/comedies', docName:'allswellthatendswell', freq:90}, Doc{ID=1651672775, docDir:'Shakespeare/tragedies', docName:'titusandronicus', freq:85}, Doc{ID=330062964, docDir:'Shakespeare/histories', docName:'kingrichardii', freq:82}, Doc{ID=2073839787, docDir:'Shakespeare/tragedies', docName:'kinglear', freq:78}, Doc{ID=500766537, docDir:'Shakespeare/poetry', docName:'sonnets', freq:76}, Doc{ID=1353411103, docDir:'Shakespeare/comedies', docName:'winterstale', freq:75}, Doc{ID=390268403, docDir:'Shakespeare', docName:'glossary', freq:73}, Doc{ID=629569265, docDir:'Shakespeare/comedies', docName:'measureforemeasure', freq:72}, Doc{ID=478568825, docDir:'Shakespeare/poetry', docName:'loverscomplaint', freq:72}, Doc{ID=1560939530, docDir:'Tolstoy', docName:'war_and_peace.txt', freq:71}, Doc{ID=230926456, docDir:'Shakespeare/comedies', docName:'comedyoferrors', freq:68}, Doc{ID=1584515256, docDir:'Shakespeare/histories', docName:'2kinghenryiv', freq:66}, Doc{ID=1767612775, docDir:'Shakespeare/comedies', docName:'midsummersnightsdream', freq:63}, Doc{ID=2054600219, docDir:'Shakespeare/comedies', docName:'cymbeline', freq:63}, Doc{ID=1864731932, docDir:'Shakespeare/comedies', docName:'tamingoftheshrew', freq:62}, Doc{ID=1764491162, docDir:'Shakespeare/poetry', docName:'various', freq:60}, Doc{ID=983496988, docDir:'Shakespeare/comedies', docName:'loveslabourslost', freq:47}, Doc{ID=745969915, docDir:'Shakespeare/tragedies', docName:'antonyandcleopatra', freq:47}, Doc{ID=1455432147, docDir:'Shakespeare/histories', docName:'3kinghenryvi', freq:44}, Doc{ID=1713597585, docDir:'Shakespeare/histories', docName:'1kinghenryvi', freq:43}, Doc{ID=1497810337, docDir:'Tolstoy', docName:'anna_karenhina.txt', freq:40}, Doc{ID=518112498, docDir:'Shakespeare/histories', docName:'kingjohn', freq:38}, Doc{ID=862744349, docDir:'Shakespeare/poetry', docName:'rapeoflucrece', freq:36}, Doc{ID=2106705121, docDir:'Shakespeare/comedies', docName:'tempest', freq:31}, Doc{ID=1184641981, docDir:'Hugo', docName:'Miserables.txt', freq:31}, Doc{ID=1531259565, docDir:'Shakespeare/comedies', docName:'merrywivesofwindsor', freq:28}, Doc{ID=630475680, docDir:'Shakespeare/histories', docName:'kinghenryviii', freq:23}, Doc{ID=1024834754, docDir:'Shakespeare/tragedies', docName:'romeoandjuliet', freq:20}, Doc{ID=1211414136, docDir:'Shakespeare/comedies', docName:'periclesprinceoftyre', freq:20}, Doc{ID=1584514866, docDir:'Shakespeare/histories', docName:'2kinghenryvi', freq:18}, Doc{ID=1713597975, docDir:'Shakespeare/histories', docName:'1kinghenryiv', freq:18}, Doc{ID=1646763189, docDir:'Shakespeare/histories', docName:'kingrichardiii', freq:16}, Doc{ID=1479279415, docDir:'Shakespeare/tragedies', docName:'othello', freq:15}, Doc{ID=1049599361, docDir:'Hugo', docName:'NotreDame_De_Paris.txt', freq:15}, Doc{ID=1946807979, docDir:'Shakespeare/comedies', docName:'twelfthnight', freq:13}, Doc{ID=162280924, docDir:'Shakespeare/comedies', docName:'asyoulikeit', freq:13}, Doc{ID=1746469475, docDir:'Shakespeare', docName:'README', freq:8}, Doc{ID=463475120, docDir:'Shakespeare/comedies', docName:'twogentlemenofverona', freq:7}, Doc{ID=844395280, docDir:'Shakespeare/tragedies', docName:'macbeth', freq:5}, Doc{ID=1889421663, docDir:'Shakespeare/histories', docName:'kinghenryv', freq:5}, Doc{ID=264479604, docDir:'Shakespeare/comedies', docName:'muchadoaboutnothing', freq:4}, Doc{ID=827479467, docDir:'Shakespeare/poetry', docName:'venusandadonis', freq:2}, Doc{ID=382588277, docDir:'Shakespeare/tragedies', docName:'coriolanus', freq:1}]}
true
         */



//        IndexTerm tmp;
//        while(!pq.isEmpty()){
//            tmp = pq.poll();
//            System.out.println(tmp);
//            int count = tmp.getFrequency();
//            int tmpCount = 0;
//            TreeSet<IndexDocument> map = tmp.getOccurrences();
//            tmpCount += map.stream().mapToInt(IndexDocument::getFrequency).sum();
//        }

        System.out.println("Before");
        System.out.println(pq);

        String ser = base64Encode(pq);

        System.out.println("encoded:");
        System.out.println(ser);


        PriorityQueue<IndexTerm> loaded = (PriorityQueue<IndexTerm>) fromString(ser);
        System.out.println("reconstructed:");
        System.out.println(loaded);

        System.out.println("eq: " + pq.equals(loaded));
        System.out.println("eq: " + pq.toString().equals(loaded.toString()));





    }

    /** Read the object from Base64 string. */
    private static Object fromString( String s ) throws IOException, ClassNotFoundException {
        byte [] data = Base64.getDecoder().decode( s );
        ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(  data ) );
        Object o  = ois.readObject();
        ois.close();
        return o;
    }

    /** Write the object to a Base64 string. */
    private static String base64Encode( Serializable o ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream( baos );
        oos.writeObject( o );
        oos.close();
        return Base64.getEncoder().encodeToString(baos.toByteArray());
    }
}
