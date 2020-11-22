 /**
 * SearchEngineGUI.java
 *
 * @author Steven Montalbano
 */

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;

import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SearchEngineGUI {
    private static final Logger logger = Logger.getLogger(SearchEngineGUI.class.getName());

    // Environment Var Setup
    private static String ASSET_PATH;   // TODO I may want to hard code these instead of reading each time
    private static String GCP_AUTH_PATH;
    private static String BUCKET_NAME;
    private static String JOB_OUTPUT_PATH = "/Users/StevenMontalbano/Programs/cs1660/FinalProject/part-r-00000";    //TODO set this somewhere
    private static LinkedHashMap<String, IndexTerm> invertedIndices;    // TODO make sure this class is packaged

    // String names for each Card that is used to change which menu is being displayed
    private static final String LOAD_SCREEN = "Let's User Select Which Files to Load";
    private static final String MAIN_MENU = "Let's User Choose to Search a Term or Calculate Top-N Terms";
    private static final String SEARCH_MENU = "Let's User Choose to Search a Term";
    private static final String SEARCH_RESULT = "Show Search Term Results";
    private static final String TOP_N_MENU = "Let's User Calculate the Top N Frequent Terms";
    private static final String TOP_N_RESULT = "Show Top N Results";

    private static final String[] searchColNames = {"Document ID", "Document Folder", "Document Name", "Occurances"};
    private static final String[] topNColNames = {"Term", "Total Occurances"};
    private static long startTime = 0L;

    private static List<String> selectedFiles;              // TODO clear this on each return to main screen

    private static DefaultListModel<String> listModel;      // Model to display the source file selection
    private static TableModel termTableModel;               // Search Term custom, immutable, TableModel instance
    private static TableModel topNTableModel;               // TopN custom, immutable, TableModel instance

    private final ButtonListener listener;                  // Custom ActionListener class to handle all button click events

    private final JFrame window;                            // Main window that displays all content
    private final JList<String> fileList;

    private final JPanel loadPanel, contentCards, searchPrompt, topNPrompt, topNResults, menuPanel, searchResult;
    private final JTextField searchTermInput, nSearchTerm;
    private final JScrollPane tableScrollPane, nResScrollPane, fileScrollPane;

    private final JLabel selectionLabel, titleLabel1, titleLabel2, titleLabel3, mainText, lblPleaseSelectAn,
            nSearchLabel, searchTerm, searchTime, topNSearch, searchLabel, searchText, topNSearchTime;

    private final JButton backToMenu,  backToMenu2, loadButton, constructButton, searchButton, topNButton, searchEnter,
            backToSearch, nSearchEnter, backToNSearch;

    private final JTable searchResTable, topNTable;

    private final Set<String> stopWords = Stream.of(
            "the", "of", "and", "a", "to", "in", "is", "you", "that", "if", "but", "or", "my", "his", "her", "he",
            "she", "i", "with", "for", "it", "this", "by", "as", "was", "had", "not", "him", "be", "at", "on", "your"
        ).collect(Collectors.toCollection(HashSet::new));


    private static Object[][] tmpData2 = {                // TODO don't make these literal instantiations
            {"THESE ARE", "FAKE RESULTS"},
            {"histories", new Integer(5000)},
            {"histories", new Integer(3000)},
            {"Blhistoriesack", new Integer(2000)},
            {"histories", new Integer(1000)},
    };

    private static Object[][] tmpData = {                // TODO don't make these literal instantiations
            {0, "THESE ARE", "FAKE RESULTS", new Integer(5)},
            {1, "histories", "1kinghenryiv", new Integer(5)},
            {2, "histories", "1kinghenryiv", new Integer(3)},
            {3, "Blhistoriesack", "2kinghenryiv", new Integer(2)},
            {4, "histories", "2kinghenryiv", new Integer(20)},
    };


    public SearchEngineGUI() {

        logger.info("SearchEngineGUI Initialized");

        listener = new ButtonListener();

        // Main window that renders all of the content
        window = new JFrame("CS 1660 Search Engine");
        window.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        window.setSize(547, 499);
        window.getContentPane().setLayout(null);
        window.setLocationRelativeTo(null);

        // Content cards are used to swap the visable content between the different application screens
        contentCards = new JPanel();
        contentCards.setBounds(6, 6, 534, 466);
        window.getContentPane().add(contentCards);
        contentCards.setLayout(new CardLayout(0, 0));

        // First menu where user specifies which files to submit to the job
        loadPanel = new JPanel();
        contentCards.add(loadPanel, LOAD_SCREEN);
        loadPanel.setLayout(null);

        mainText = new JLabel("Load CS 1660 Search Engine");
        mainText.setHorizontalAlignment(SwingConstants.CENTER);
        mainText.setFont(new Font("Lucida Grande", Font.BOLD, 16));
        mainText.setBounds(106, 27, 325, 45);
        loadPanel.add(mainText);

        loadButton = new JButton("Choose Files");
        loadButton.addActionListener(listener);
        loadButton.setBounds(202, 351, 138, 29);
        loadPanel.add(loadButton);

        constructButton = new JButton("Construct Inverted Indices");
        constructButton.setEnabled(false);
        constructButton.setBounds(140, 387, 265, 29);
        constructButton.addActionListener(listener);
        loadPanel.add(constructButton);

        selectionLabel = new JLabel("Files Selected");
        selectionLabel.setHorizontalAlignment(SwingConstants.CENTER);
        selectionLabel.setBounds(191, 68, 149, 16);
        selectionLabel.setVisible(false);
        loadPanel.add(selectionLabel);

        fileScrollPane = new JScrollPane();
        fileScrollPane.setBounds(43, 114, 449, 225);
        loadPanel.add(fileScrollPane);

        fileList = new JList<String>();
        fileList.setModel(listModel);
        fileScrollPane.setViewportView(fileList);

        menuPanel = new JPanel();
        contentCards.add(menuPanel, MAIN_MENU);
        menuPanel.setLayout(null);

        titleLabel1 = new JLabel("CS 1660 Search Engine was Loaded");
        titleLabel1.setFont(new Font("Lucida Grande", Font.BOLD, 15));
        titleLabel1.setHorizontalAlignment(SwingConstants.CENTER);
        titleLabel1.setBounds(65, 36, 398, 52);
        menuPanel.add(titleLabel1);

        titleLabel2 = new JLabel("and");
        titleLabel2.setFont(new Font("Lucida Grande", Font.BOLD, 15));
        titleLabel2.setHorizontalAlignment(SwingConstants.CENTER);
        titleLabel2.setBounds(111, 73, 299, 40);
        menuPanel.add(titleLabel2);

        titleLabel3 = new JLabel("Inverted Indices were Successfully Constructed");
        titleLabel3.setFont(new Font("Lucida Grande", Font.BOLD, 15));
        titleLabel3.setHorizontalAlignment(SwingConstants.CENTER);
        titleLabel3.setBounds(54, 94, 421, 52);
        menuPanel.add(titleLabel3);

        // Operation Menu: Select either Search Term or Top-N job to Hadoop cluster
        searchButton = new JButton("Search for Term");
        searchButton.addActionListener(listener);
        searchButton.setBounds(94, 303, 341, 40);
        menuPanel.add(searchButton);

        topNButton = new JButton("Calculate Top-N");
        topNButton.addActionListener(listener);
        topNButton.setBounds(94, 251, 341, 40);
        menuPanel.add(topNButton);

        lblPleaseSelectAn = new JLabel("Please Select an Action");
        lblPleaseSelectAn.setFont(new Font("Lucida Grande", Font.PLAIN, 18));
        lblPleaseSelectAn.setHorizontalAlignment(SwingConstants.CENTER);
        lblPleaseSelectAn.setBounds(152, 182, 234, 57);
        menuPanel.add(lblPleaseSelectAn);

        // Search Term Menu
        searchPrompt = new JPanel();
        contentCards.add(searchPrompt, SEARCH_MENU);
        searchPrompt.setLayout(null);

        searchLabel = new JLabel("Enter Your Search Term");
        searchLabel.setFont(new Font("Lucida Grande", Font.BOLD, 15));
        searchLabel.setHorizontalAlignment(SwingConstants.CENTER);
        searchLabel.setBounds(154, 129, 220, 48);
        searchPrompt.add(searchLabel);

        searchTermInput = new JTextField();
        searchTermInput.setBounds(174, 230, 178, 36);
        searchPrompt.add(searchTermInput);
        searchTermInput.setColumns(10);

        searchEnter = new JButton("Search");
        searchEnter.setBounds(210, 312, 117, 29);
        searchEnter.addActionListener(listener);
        searchPrompt.add(searchEnter);

        // Top-N Search Menu
        topNPrompt = new JPanel();
        contentCards.add(topNPrompt, TOP_N_MENU);
        topNPrompt.setLayout(null);

        nSearchLabel = new JLabel("Enter Your N Value");
        nSearchLabel.setBounds(171, 125, 186, 19);
        nSearchLabel.setHorizontalAlignment(SwingConstants.CENTER);
        nSearchLabel.setFont(new Font("Lucida Grande", Font.BOLD, 15));
        topNPrompt.add(nSearchLabel);

        nSearchTerm = new JTextField();
        nSearchTerm.setBounds(196, 190, 130, 26);
        nSearchTerm.setColumns(10);
        topNPrompt.add(nSearchTerm);

        nSearchEnter = new JButton("Search");
        nSearchEnter.setBounds(219, 264, 85, 29);
        nSearchEnter.addActionListener(listener);
        topNPrompt.add(nSearchEnter);

        // Search Term result view
        searchResult = new JPanel();
        contentCards.add(searchResult, SEARCH_RESULT);
        searchResult.setLayout(null);

        searchText = new JLabel("You Searched For the Term:");
        searchText.setBounds(22, 71, 202, 38);
        searchResult.add(searchText);

        searchTime = new JLabel("Your Search was Executed in <> ms");
        searchTime.setBounds(22, 41, 279, 38);
        searchResult.add(searchTime);

        searchTerm = new JLabel("<TERM>");
        searchTerm.setHorizontalAlignment(SwingConstants.CENTER);
        searchTerm.setFont(new Font("Lucida Grande", Font.BOLD | Font.ITALIC, 13));
        searchTerm.setBounds(22, 106, 228, 29);
        searchResult.add(searchTerm);

        backToSearch = new JButton("Return to Search");
        backToSearch.addActionListener(listener);
        backToSearch.setBounds(342, 41, 174, 29);
        searchResult.add(backToSearch);

        backToMenu = new JButton("Return to Menu");
        backToMenu.addActionListener(listener);
        backToMenu.setBounds(342, 83, 174, 29);
        searchResult.add(backToMenu);

        tableScrollPane = new JScrollPane();
        tableScrollPane.setBounds(22, 160, 491, 277);
        searchResult.add(tableScrollPane);

        searchResTable = new JTable(tmpData, searchColNames);
        searchResTable.setModel(termTableModel);
        tableScrollPane.setViewportView(searchResTable);

        // Top-N result view
        topNResults = new JPanel();
        contentCards.add(topNResults, TOP_N_RESULT);
        topNResults.setLayout(null);

        backToNSearch = new JButton("Return to Search");
        backToNSearch.addActionListener(listener);
        backToNSearch.setBounds(331, 22, 175, 29);
        topNResults.add(backToNSearch);

        backToMenu2 = new JButton("Return to Menu");
        backToMenu2.addActionListener(listener);
        backToMenu2.setBounds(331, 63, 175, 29);
        topNResults.add(backToMenu2);

        topNSearch = new JLabel("Top <> Frequent Terms:");
        topNSearch.setBounds(25, 54, 193, 38);
        topNResults.add(topNSearch);

        topNSearchTime = new JLabel("Calculation was Executed in <> ms");
        topNSearchTime.setBounds(25, 13, 294, 38);
        topNResults.add(topNSearchTime);

        nResScrollPane = new JScrollPane();
        nResScrollPane.setBounds(28, 112, 478, 324);
        topNResults.add(nResScrollPane);

        topNTable = new JTable(tmpData2, topNColNames);
        topNTable.setModel(topNTableModel);
        nResScrollPane.setViewportView(topNTable);

        // Once all components are initialized, display the main GUI window
        window.setVisible(true);
    }

    public static void main(String[] args) {

        try {
            logger.info("Performing initial setup...");
            setUp();
        } catch (Exception e) {
            logger.severe("SearchEngineGUI raised an Exception during the setup() method:");
            logger.log(Level.SEVERE, e.getMessage(), e);
            System.exit(1);
        }

        logger.info("Successfully initialized the global variables. Constructing the SearchEngineGUI now...");

        try {
            new SearchEngineGUI();
        } catch (Exception e) {
            logger.info("SearchEngineGUI raised an Exception during execution:");
            logger.log(Level.SEVERE, e.getMessage(), e);
            System.exit(1);

        }
    }

    private enum JobType {
        Construct,
        Search,
        TopN
    }

    String outputPath = "OUTPUT_PATH"; //TODO


    private static void submitJob(JobType job, List<String> inputFiles, Object query) {

        System.out.println("Submit Job called");

        // TODO create new input dir in GCP bucket and move selected files into it
        // TODO create a results directory in GCP bucket


        String hadoopCmd = null;

        switch (job) {
            case Construct:
                logger.info("Constructing Inverted Indices with the following files:");
                logger.info(inputFiles.toString());
                hadoopCmd = "hadoop jar InvertedIndices.jar <INPUT:%s> <OUTPUT:%s>";
                // TODO call GCP
                break;
            case Search:
                logger.info("Searching for the following term:" + query);
                hadoopCmd = String.format("hadoop jar SearchTerm.jar <INPUT> <OUTPUT>", query);

                // TODO call GCP
                break;
            case TopN:
                logger.info("Searching for the top " + query + " terms");
                hadoopCmd = String.format("hadoop jar TopN.jar -D N=%s <INPUT> <OUTPUT>", query);
                // TODO call GCP
                break;
        }

        System.out.println("Mock hadoop job call:");
        System.out.println(hadoopCmd);

    }

    private static void getJobResults(JobType job, Object query) {

        // TODO read results from the local docker container to get the rendering/parsing logic working

        // TODO read GCP storage bucket @ OUTPUT_PATH
        //  Check job type, construct the table model from the results and render it in the GUI


        // TODO after returning to main menu / closing the program delete the results directory

        switch (job) {
//            case Construct:
//                logger.info("Constructing Inverted Indices with the following files:");
//                logger.info(inputFiles.toString());
//                // TODO call GCP
//                break;
            case Search:
                logger.info("Reading the InvertedIndices for the term:" + query);
                IndexTerm tmp = new IndexTerm((String)query);

//                if(invertedIndices.contains(tmp)){
//                    logger.info("Found the term! Results:");
////                    System.out.println(invertedIndices.);
//                }

                break;
            case TopN:
                logger.info("Reading the InvertedIndices for the top " + query + " terms");
                break;
        }



    }


    private static void authExplicit(String jsonPath) throws IOException {
        // TODO move these GCP objects to class attributes
        // You can specify a credential file by providing a path to GoogleCredentials.
        // Otherwise credentials are read from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath))
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();

        Bucket bucket = storage.get(BUCKET_NAME);

        System.out.println(bucket.toString());

        Blob blob = bucket.get("assets/");
        System.out.println();

//        System.out.println("Buckets:");
//        Page<Bucket> buckets = storage.list();
//        for (Bucket bucket : buckets.iterateAll()) {
//            System.out.println(bucket.toString());
//        }
    }


    private static void setUp() throws Exception {

        ASSET_PATH = System.getenv("ASSET_PATH");
        logger.info("Asset Path read from env: " + ASSET_PATH);

        GCP_AUTH_PATH = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        logger.info("GCP Credential Path read from env: " + GCP_AUTH_PATH);

        BUCKET_NAME = System.getenv("BUCKET_NAME");
        logger.info("GCP Storage Bucket name read from env: " + BUCKET_NAME);

        logger.info("Attempting to connect to Google Cloud Platform...");
//        authExplicit(GCP_AUTH_PATH);  //TODO comment out to save GCP credits

//        // Create a TreeMap to store the <Relative Path, Absolute Path> key value pairs to simplify file selection
//        srcFiles = new TreeMap<String, Path>();
//        // Read in the provided text files from the ASSET_PATH into the srcFiles TreeMap
//        loadFiles(ASSET_PATH, srcFiles);
        Scanner scan = new Scanner(new File(ASSET_PATH));

        // Set up the file selection JList Model to be immutable and allow multiple selections
        listModel = new DefaultListModel<String>();

        while(scan.hasNextLine())
            listModel.addElement(scan.nextLine());

        // TODO these need to be in their own method so that live data can be populated after the job finishes.

        termTableModel = new DefaultTableModel(tmpData, searchColNames) {
            private static final long serialVersionUID = 1L;

            public boolean isCellEditable(int row, int column) {
                return false;//This causes all cells to be not editable
            }
        };

        topNTableModel = new DefaultTableModel(tmpData2, topNColNames) {
            private static final long serialVersionUID = 1L;

            public boolean isCellEditable(int row, int column) {
                return false;//This causes all cells to be not editable
            }
        };
    }

    private static void loadFiles(String assetPath, Map<String, Path> map) {

        // Try-With-Resources statement: The Stream 'walk' is automatically closed when the finally block would normally execute.
        // Similar behavior to a Python 'with open('file'):' structure
        try (Stream<Path> walk = Files.walk(Paths.get(assetPath))) {
            walk                                                                         /*  Recursively Iterate (walk) down all sub directories of the assets using the Stream	  */
                .filter(f -> !f.toString().contains(".DS_Store"))                        /*  Filter out .DS_Store files generated by the OSX File System 						  */
                .filter(f -> !f.toString().contains("MANIFEST"))                        /*  Filter out .DS_Store files generated by the OSX File System 						  */
                .filter(Files::isRegularFile)                                            /*  Filter out non-human-readable files i.e., data files or serialized objects 		  	  */
                .forEach(f -> map.put(f.toString().split("assets/")[1], f));       /*  Filter out the absolute path to display cleaner working directory for user selection  */
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void parseInvertedIndices() throws FileNotFoundException {

        logger.info("Beginning Parsing job result file: " + JOB_OUTPUT_PATH);
        Scanner scan = new Scanner(new File(JOB_OUTPUT_PATH));

        String line;

        PriorityQueue<IndexTerm> pq = new PriorityQueue<>();
        IndexTerm term;
        IndexDocument doc;

        String word, ID, dir, name = null, freq = null;

        Pattern regex = Pattern.compile("\\{([^}]+)\\}");
        Matcher match;
        String[] pieces;

        ArrayList<IndexDocument> docList;


        while(scan.hasNextLine()){
            line = scan.nextLine().replaceAll("'", "");
            word = line.split(":", 2)[0];
            match = regex.matcher(line.split(":", 2)[1]);
            docList = new ArrayList<>();

            while(match.find()){
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




        IndexTerm[] arr = pq.toArray(new IndexTerm[pq.size()]);
        Arrays.sort(arr, pq.comparator());
        for (IndexTerm t: arr)
            invertedIndices.put(t.getTerm(), t);

        int i = 0;
        for (IndexTerm indexTerm : pq) {

            if (i++ > 15)
                break;

            term = indexTerm;
            System.out.println(term.getTerm() + ":" + term.getOccurrences().toString());
        }

    }

    private void resetText() {
        searchTime.setText("Your Search was Executed in <> ms");
        topNSearchTime.setText("Calculation was Executed in <> ms");
        topNSearch.setText("Top <> Frequent Terms:");
    }

    private void showError(String msg) {
        JOptionPane optionPane = new JOptionPane(msg, JOptionPane.ERROR_MESSAGE);
        JDialog error = optionPane.createDialog("Input Error");
        error.setAlwaysOnTop(true);
        error.setVisible(true);
    }


    private class ButtonListener implements ActionListener {  //Private inner class to keep all refs variables in scope

        /**
         * Creates a Component reference to the source of the ActionEvent that can be compared against
         * all JButton references, since they are descendants of the Component class. This allows a
         * single ActionListener function to handle all JButton Events, instead of instantiating a
         * separate ActionListener for each GUI Component.
         */
        @Override
        public void actionPerformed(ActionEvent e) {

            Component buttonRef = (Component) e.getSource();

            if (buttonRef == loadButton) {
                logger.info("Files selected:");

                selectedFiles = fileList.getSelectedValuesList();

                if (selectedFiles.isEmpty()) {
                    JOptionPane.showMessageDialog(null, "Please Select at Least 1 File to Continue.");
                    return;
                }

                logger.info(selectedFiles.toString());

                selectionLabel.setVisible(true);
                constructButton.setEnabled(true);
            }

            if (buttonRef == constructButton) {
                logger.info("Construct Indices Button Clicked, Loading Selected Files...");


                if (!selectedFiles.isEmpty()) {
                    submitJob(JobType.Construct, selectedFiles, null);
                    CardLayout cl = (CardLayout) (contentCards.getLayout());
                    cl.show(contentCards, MAIN_MENU);
                }
            }

            if (buttonRef == searchButton) {
                logger.info("Showing Search Menu");

                CardLayout cl = (CardLayout) (contentCards.getLayout());
                cl.show(contentCards, SEARCH_MENU);
            }

            if (buttonRef == searchEnter) {

                String term;

                if (searchTermInput.getText().length() == 0) {
                    showError("Please Enter a Search Term to Continue.");
                    return;
                }

                term = searchTermInput.getText().toLowerCase();

                if (stopWords.contains(term)) {
                    showError("The search term you entered is a Stop Word, there will be no results.");
                    return;
                }

                submitJob(JobType.Search, selectedFiles, term);

                logger.info("Showing Search Results for Term: " + term);

                // Disable input while search job is executing
                searchTermInput.setEnabled(false);
                searchEnter.setEnabled(false);

                //TODO show loading msg


                searchTerm.setText(term);
                startTime = System.currentTimeMillis();

                // TODO not updating while main thread sleeps, use SwingUtilities.invokeLater() for the hadoop call
//				searchStatus.setEnabled(true);
//				searchStatus.setBackground(Color.BLUE);

                //TODO mocked job execution: call method to submit job to Hadoop
                try {
//					searchStatus.setText("Connecting to Hadoop Cluster...\n");
//					searchStatus.setText(searchStatus.getText().concat("Submitting Search Job for Term" + searchInput.getText() + "\n"));
//					searchStatus.setText(searchStatus.getText().concat("Job Finishing..."));
                    Thread.sleep(1500);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }

                searchTime.setText(searchTime.getText().replace("<>", Long.valueOf(System.currentTimeMillis() - startTime).toString()));

                CardLayout cl = (CardLayout) (contentCards.getLayout());
                cl.show(contentCards, SEARCH_RESULT);
            }

            if (buttonRef == backToSearch) {
                logger.info("Returning to Search Menu");

                // Re-enable input for next search job
                searchTermInput.setEnabled(true);
                searchEnter.setEnabled(true);

                CardLayout cl = (CardLayout) (contentCards.getLayout());
                cl.show(contentCards, SEARCH_MENU);
            }

            if (buttonRef == backToNSearch) {
                logger.info("Returning to Top N Menu");

                // Re-enable input for next search job
                nSearchTerm.setEnabled(true);
                nSearchEnter.setEnabled(true);

                // Reset Text back to template form
                resetText();

                CardLayout cl = (CardLayout) (contentCards.getLayout());
                cl.show(contentCards, TOP_N_MENU);
            }

            if (buttonRef == backToMenu || buttonRef == backToMenu2) {
                logger.info("Returning to Main Menu");

                // Reset JLabels to have <> in place of search parameters so that the next replace method does not fail
                resetText();

                // Re-enable input for next search job
                searchTermInput.setEnabled(true);
                searchEnter.setEnabled(true);
                nSearchTerm.setEnabled(true);
                nSearchEnter.setEnabled(true);

                CardLayout cl = (CardLayout) (contentCards.getLayout());
                cl.show(contentCards, MAIN_MENU);
            }


            if (buttonRef == topNButton) {
                logger.info("Showing Top N Menu");

                CardLayout cl = (CardLayout) (contentCards.getLayout());
                cl.show(contentCards, TOP_N_MENU);
            }


            if (buttonRef == nSearchEnter) {

                int N;

                if (nSearchTerm.getText().length() == 0) {
                    showError("Please Enter an N Value to Continue.");
                    return;
                } else {

                    try {
                        N = Integer.parseInt(nSearchTerm.getText());
                    } catch (NumberFormatException ex) {
                        logger.info("Exception Caught: " + ex.toString());
                        showError("Please Enter N as in Integer Value to Continue.");
                        return;
                    }
                }

                logger.info("Showing Results for Top N = " + N + " Terms");

                submitJob(JobType.TopN, selectedFiles, N);


                topNSearch.setText(topNSearch.getText().replace("<>", String.valueOf(N)));

                // Disable input while search job is executing
                nSearchTerm.setEnabled(false);
                nSearchEnter.setEnabled(false);

                startTime = System.currentTimeMillis();


                //TODO mocked job execution: call method to submit job to Hadoop
                try {
// 					searchStatus.setText("Connecting to Hadoop Cluster...\n");
// 					searchStatus.setText(searchStatus.getText().concat("Submitting Search Job for Term" + searchInput.getText() + "\n"));
// 					searchStatus.setText(searchStatus.getText().concat("Job Finishing..."));
                    Thread.sleep(1500);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }


                topNSearchTime.setText(topNSearchTime.getText().replace("<>", Long.valueOf(System.currentTimeMillis() - startTime).toString()));

                CardLayout cl = (CardLayout) (contentCards.getLayout());
                cl.show(contentCards, TOP_N_RESULT);

            }

        } // End ActionPerformed
    } // End ButtonListener class
} // End SearchEngineGUI class
