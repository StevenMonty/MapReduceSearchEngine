 /**
 * SearchEngineGUI.java
 *
 * @author Steven Montalbano
 */
import javax.swing.*;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableModel;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SearchEngineGUI {
    private static final Logger logger = Logger.getLogger(SearchEngineGUI.class.getName());

    private static ResultParser resParser;
    private static JobExecutors.InvertedIndexExecutor jobExecutors;

    // Environment Var Setup
    private static String FILE_LIST_PATH;   // TODO I may want to hard code these instead of reading each time
    private static String GCP_AUTH_PATH;
    private static String BUCKET_NAME;
//    private static String JOB_OUTPUT_PATH = "/Users/StevenMontalbano/Programs/cs1660/FinalProject/part-r-00000";    //TODO set this somewhere
    private static LinkedHashMap<String, IndexTerm> invertedIndices = null;    // TODO make sure this class is packaged

    // String names for each Card that is used to change which menu is being displayed
    private static final String LOAD_SCREEN = "Let's User Select Which Files to Load";
    private static final String MAIN_MENU = "Let's User Choose to Search a Term or Calculate Top-N Terms";
    private static final String SEARCH_MENU = "Let's User Choose to Search a Term";
    private static final String SEARCH_RESULT = "Show Search Term Results";
    private static final String TOP_N_MENU = "Let's User Calculate the Top N Frequent Terms";
    private static final String TOP_N_RESULT = "Show Top N Results";
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
    private static JDialog loading;

    private final Set<String> stopWords = Stream.of(
            "the", "of", "and", "a", "to", "in", "is", "you", "that", "if", "but", "or", "my", "his", "her", "he",
            "she", "i", "with", "for", "it", "this", "by", "as", "was", "had", "not", "him", "be", "at", "on", "your"
        ).collect(Collectors.toCollection(HashSet::new));

    private static JTable searchResTable, topNTable;

    private static final String[] searchColNames = {"Document ID", "Document Folder", "Document Name", "Occurrences"};
    private static final String[] topNColNames = {"Rank" , "Term", "Total Occurrences"};

    private static Object[][] topNTableData = new Object[3][3];

    private static Object[][] searchTermTableData = new Object[4][4];


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

        loading = new JDialog();
        loading.setLayout(null);
        loading.setAlwaysOnTop(true);
        loading.setLocationRelativeTo(null);
        loading.setVisible(false);

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

        searchResTable = new JTable(searchTermTableData, searchColNames);
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

        topNTable = new JTable(topNTableData, topNColNames);
        nResScrollPane.setViewportView(topNTable);

        window.addWindowListener(new WindowAdapter() {
            /**
             * On WindowClosing event, delete the /input and /output directories in the
             * Google Storage bucket.
             *
             * @param e
             */
            public void windowClosing(WindowEvent e) {
                System.out.println("Main App Window Closed, Running CleanUp()...");
                window.setVisible(false);
                if (jobExecutors != null)
                    jobExecutors.cleanUpBucket();
            }
        });

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

    private static void submitJob(JobType job, List<String> inputFiles, Object query) throws Exception {

        System.out.println("Submit Job called");

        //TODO make construct its own method since render results is doing the same thing as this func
            switch (job) {
                case Construct:
                    logger.info("Constructing Inverted Indices with the following files:");
                    logger.info(inputFiles.toString());
                    startTime = System.currentTimeMillis();

                    jobExecutors.runJob();
                    System.out.printf("Done constructing InvertedIndices, job completed in %s seconds\n", Long.valueOf((System.currentTimeMillis() - startTime)/100).toString());

                    startTime = System.currentTimeMillis();

                    invertedIndices = jobExecutors.parseResults();
                    System.out.printf("Done parsing Hadoop Job results, completed in %s ms\n", Long.valueOf(System.currentTimeMillis() - startTime).toString());
                    break;
                case Search:
                    logger.info("Searching for the following term:" + query);
                    getJobResults(job, query);
                    return;
                case TopN:
                    logger.info("Searching for the top " + query + " terms");
                    getJobResults(job, query);
                    break;
            }
    }

    private static void getJobResults(JobType job, Object query) throws NoResultException {
        switch (job) {
            case Search:
                query = query.toString();
                System.out.println("Reading the InvertedIndices for the term:" + query);
                if(invertedIndices.containsKey(query)){
                    System.out.println("Found the search term!");
                    renderQueryResults(job, invertedIndices.get(query));
                } else {
                    System.out.println("Term not found!"); //TODO display error?
                    throw new NoResultException("The search term '" + query + "' was not found in any documents, and was not in the StopWord list.");
                }
                break;
            case TopN:
                query = (Integer) query;
                logger.info("Reading the InvertedIndices for the top " + query + " terms");
                renderQueryResults(job, query);
                break;
        } // end switch

    } // end getJobResults

    /**
     * Create and renders the JTable that displays the query results.
     * @param job
     * @param results
     */
    private static void renderQueryResults(JobType job, Object results){
        int i = 0;
        IndexTerm term;
        IndexDocument doc;
        switch (job) {
            case Search:
                term = (IndexTerm)results;
                System.out.println("renderQueryResults (term): " + term);
                searchTermTableData = new Object[term.getOccurrences().size()][4];
                Iterator<IndexDocument> termIter = term.getOccurrences().stream().iterator();

                while(termIter.hasNext()){
                    doc = termIter.next();
                    searchTermTableData[i][0] = doc.getDocID();
                    searchTermTableData[i][1] = doc.getDocDir();
                    searchTermTableData[i][2] = doc.getDocName();
                    searchTermTableData[i][3] = doc.getFrequency();
                    i++;
                }

                termTableModel = new DefaultTableModel(searchTermTableData, searchColNames) {
                    private static final long serialVersionUID = 1L;

                    public boolean isCellEditable(int row, int column) {
                        return false;//This causes all cells to be not editable
                    }
                };

                searchResTable.setModel(termTableModel);
                searchResTable.repaint();
                break;

            case TopN:
                Integer N = (Integer) results;
                System.out.println("renderQueryResults (topN): " + N);

                topNTableData = new Object[N][3];
                Iterator<IndexTerm> topNIter = invertedIndices.values().iterator();

                while(i < N && topNIter.hasNext()){
                    term = topNIter.next();
                    topNTableData[i][0] = i+1;  // List this term at Pos 1, being most frequent, but inserting as index 0
                    topNTableData[i][1] = term.getTerm();
                    topNTableData[i][2] = term.getFrequency();
                    i++;
                }

                topNTableModel = new DefaultTableModel(topNTableData, topNColNames) {
                    private static final long serialVersionUID = 1L;

                    public boolean isCellEditable(int row, int column) {
                        return false;//This causes all cells to be not editable
                    }
                };

                topNTable.setModel(topNTableModel);
                topNTable.repaint();
                break;
        } // end switch

    } // end renderQueryResults

    private static void setUp() throws Exception {

        FILE_LIST_PATH = System.getenv("FILE_LIST_PATH");
        logger.info("Asset Path read from env: " + FILE_LIST_PATH);

        GCP_AUTH_PATH = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
        logger.info("GCP Credential Path read from env: " + GCP_AUTH_PATH);

        BUCKET_NAME = System.getenv("BUCKET_NAME");
        logger.info("GCP Storage Bucket name read from env: " + BUCKET_NAME);

        logger.info("Attempting to connect to Google Cloud Platform...");

        Scanner scan = new Scanner(new File(FILE_LIST_PATH));

        // Set up the file selection JList Model to be immutable and allow multiple selections
        listModel = new DefaultListModel<String>();

        while(scan.hasNextLine())
            listModel.addElement(scan.nextLine());
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

    private void resetText() {
        searchTime.setText("Your Search was Executed in <> ms");
        topNSearchTime.setText("Calculation was Executed in <> ms");
        topNSearch.setText("Top <> Frequent Terms:");
    }


    private static void showError(String msg) {
        JOptionPane optionPane = new JOptionPane(msg, JOptionPane.ERROR_MESSAGE);
        JDialog error = optionPane.createDialog("Input Error");
        error.setAlwaysOnTop(true);
        error.setVisible(true);
    }

    private static JDialog showAlert(String msg) {
        JOptionPane optionPane = new JOptionPane(msg, JOptionPane.INFORMATION_MESSAGE);
        JDialog error = optionPane.createDialog("Input Error");
        error.setAlwaysOnTop(true);
        error.setVisible(false);
        error.setEnabled(false);
        return error;
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

                try {
                    jobExecutors = new JobExecutors.InvertedIndexExecutor(selectedFiles);
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
                logger.info(selectedFiles.toString());

                selectionLabel.setVisible(true);
                constructButton.setEnabled(true);
            }

            if (buttonRef == constructButton) {
                logger.info("Construct Indices Button Clicked, Loading Selected Files...");


                if (!selectedFiles.isEmpty()) {
                    try {
                        submitJob(JobType.Construct, selectedFiles, null);
                    } catch (NoResultException ex2) {
                        showError("NoResultException Raised during invertedIndex construction? that shouldn't even be possible...?");
                        ex2.printStackTrace();
                        return;
                    } catch (Exception ex1) {
                        ex1.printStackTrace();
                    }

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

                try {
                    startTime = System.currentTimeMillis();
                    submitJob(JobType.Search, selectedFiles, term);
                } catch (FileNotFoundException fileNotFoundException) {
                    fileNotFoundException.printStackTrace();
                }  catch (NoResultException ex2) {
                    showError(ex2.getMessage());
                    return;
                } catch (Exception ioException) {
                    ioException.printStackTrace();
                }

                logger.info("Showing Search Results for Term: " + term);

                // Disable input while search job is executing
                searchTermInput.setEnabled(false);
                searchEnter.setEnabled(false);


                searchTerm.setText(term);

                // TODO not updating while main thread sleeps, use SwingUtilities.invokeLater() for the hadoop call

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

                    if (N <= 0){
                        showError("Searching for the TopN terms with a N value <= 0 will yield no results.\nPlease enter a positive integer.");
                        return;
                    }
                }

                logger.info("Showing Results for Top N = " + N + " Terms");

                topNSearch.setText(topNSearch.getText().replace("<>", String.valueOf(N)));

                // Disable input while search job is executing
                nSearchTerm.setEnabled(false);
                nSearchEnter.setEnabled(false);

                startTime = System.currentTimeMillis();

                try {
                    getJobResults(JobType.TopN, N);
                } catch (NoResultException noResultException) {
                    noResultException.printStackTrace();
                }

                topNSearchTime.setText(topNSearchTime.getText().replace("<>", Long.valueOf(System.currentTimeMillis() - startTime).toString()));

                CardLayout cl = (CardLayout) (contentCards.getLayout());
                cl.show(contentCards, TOP_N_RESULT);

            }

        } // End ActionPerformed
    } // End ButtonListener class
} // End SearchEngineGUI class
