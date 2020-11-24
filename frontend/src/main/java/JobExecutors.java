import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.Dataproc;
import com.google.common.collect.ImmutableList;
import com.google.cloud.dataproc.v1.*;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SwingWorker class to set up the directory structure and start the InvertedIndex job outside of
 * the main Swing EventDispatchThread to keep the GUI responsive.
 */

public class JobExecutors {
    static LinkedHashMap<String, IndexTerm> hMap;
    static boolean isDone;
    static String GCP_AUTH_PATH;
    static String jobResultStr;
    static String BUCKET_NAME;
    static String PROJECT_ID;
    static String REGION;
    static String CLUSTER_NAME;
    static String BUCKET_ASSET_PATH;
    static String JOB_INPUT_DIR;
    static String JOB_OUTPUT_DIR;
    static List<String> inputFiles;
    static Storage storage;
    static Dataproc dataproc;

    static class InvertedIndexExecutor {

        public InvertedIndexExecutor(List<String> srcFiles) throws IOException {
            isDone = false;
            inputFiles = srcFiles;
            GCP_AUTH_PATH = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
            BUCKET_NAME = System.getenv("BUCKET_NAME");
            BUCKET_ASSET_PATH = System.getenv("BUCKET_ASSET_PATH");
            JOB_INPUT_DIR = System.getenv("JOB_INPUT_DIR");
            JOB_OUTPUT_DIR = System.getenv("JOB_OUTPUT_DIR");
            PROJECT_ID = System.getenv("PROJECT_ID");
            REGION = System.getenv("REGION");
            CLUSTER_NAME = System.getenv("CLUSTER_NAME");
            storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
            dataproc  = new Dataproc.Builder(new NetHttpTransport(),
                        new JacksonFactory(),
                        new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault()
                                .createScoped(DataprocScopes.all()))).setApplicationName("CS1660SearchEngine").build();
        }

        /**
         * Method that encapsulates all the Mapreduce input/output setup, execution, and post-processing.
         *
         * Moved the user selected files from the the /assets folder of the source bucket, specified by
         * the BUCKET_NAME env var, to the /input directory on the same bucket that will serve as the input
         * dir to the MapReduce job.
         *
         * Executes the InvertedIndex MapReduce Job using the InvertedIndex.jar file in the root of the same
         * Google Storage Bucket.
         *
         * Downloads all of the result files and merges them into a single String object that can then be parsed into
         * the final InvertedIndex for the selected files.
         */
        protected void runJob() {

            try{
                System.out.println("Moving selected input files to the BUCKET/input dir...");

                // move the selected files from the bucket's /assets dir to /input
                moveInputFiles(inputFiles);

                System.out.println("Starting the InvertedIndex MapReduceTask...");

                // run the InvertedIndex job
                submitHadoopMapReduceJob(PROJECT_ID, REGION, CLUSTER_NAME);

                // TODO get this working: run HadoopFS job to merge output files -> hadoop fs -getmerge /output/dir/on/hdfs/ /desired/local/output/file.txt

                System.out.println("Downloading and merging the InvertedIndex results...");

                // copy the output files from the bucket to local machine for processing
                downloadResults("output");

            } catch (Exception e) {
                System.err.println("Exception raised during Hadoop Operations: " + e.getMessage());
                e.printStackTrace();
            }
        }

        /**
         * Utility function to parse the MapReduce Job results from a large, multi-line String returned by
         * downloadResults() into a LinkedHashMap with a String (the indexed word) as the Key, and IndexTerm
         * object as the Value.
         *
         * Since the jobResultString is an object representation of the Hadoop part-r-xxxxx files, which are
         * new-line delimited, a Scanner is used to parse the String as if it were reading the result files
         * directly.
         *
         * Builds an intermediate PriorityQueue to store the IndexTerm objects and all the IndexDocument
         * objects, which represent each occurrence of that term in the job input files. Since the IndexTerm
         * implements the Comparable<IndexTerm> interface, the IndexTerms are automatically sorted in descending
         * order according to their total occurrences across all IndexDocuments. This effectively creates a TopN
         * data structure to process those types of queries without the need for another MapReduce job to execute.
         *
         * Once the PriorityQueue is finished being built from the jobResultString, it is polled() to get the most
         * frequent term, which is then inserted into a LinkedHashMap structure. Since it maintains insertion order,
         * it's Iterator is automatically sorted by most frequent terms, which allows TopN queries to be performed by
         * iterating over the first N values in the entrySet. This also allows to O(1) SearchTerm queries,
         * since the query String will be a key into the Map, and the IndexTerm returned contains a List<IndexDocument>
         * of every document processed during this job that that specific term appeared in. Both user query types are
         * contained in the functionality of the LinkedHashMap, so only one MapReduce job is necessary, which
         * significantly increased overall application performance.
         *
         * @return a LinkedHashMap<String, IndexTerm> which represents the final constructed InvertedIndex structure
         */
        protected LinkedHashMap<String, IndexTerm> parseResults() {

            hMap = new LinkedHashMap<>();
            try {
                Scanner scan = new Scanner(jobResultStr);

                IndexTerm term;
                IndexDocument doc;
                String line, word, ID, dir, name, freq;
                String[] pieces;
                Matcher match;

                // Regex for parsing the String representation of an IndexTerm that the Reducer outputs
                Pattern regex = Pattern.compile("\\{([^}]+)\\}");


                // Intermediate IndexTerm storage to maintain the TopN ordering of terms.
                PriorityQueue<IndexTerm> pq = new PriorityQueue<>();

                // Every document the current IndexTerm appeared in
                ArrayList<IndexDocument> docList;

                while (scan.hasNextLine()) {
                    docList = new ArrayList<>();
                    // String processing and cleanup
                    line = scan.nextLine().replaceAll("'", "");
                    word = line.split(":", 2)[0];

                    // Pull out all individual data fields of the IndexTerm so that it can be reconstructed.
                    match = regex.matcher(line.split(":", 2)[1]);

                    // Since the Reduce output format is <TERM>: [DOC-1, DOC-2,...],
                    // this parses each document this term appeared in, and reconstructs the document list
                    // E.g.,  <TERM>: [Doc{ID:164, Dir:'Shakespeare/histories', Name:'kingrichardiii', Freq:306}, Doc{ID:164,...}, ...]
                    while (match.find()) {
                        pieces = match.group().replaceAll("\\{*\\}*", "").split("[,:]+");
                        ID = pieces[1];
                        dir = pieces[3];
                        name = pieces[5];
                        freq = pieces[7];
                        docList.add(new IndexDocument(name, dir, freq, ID));    // IndexDocument(String docName, String docDir, int frequency, int docID)
                    }

                    // Create new IndexTerm for this word parsed
                    term = new IndexTerm(word);
                    // Add all IndexDocument occurrences
                    term.addDocuments(docList);
                    // Add to the PQ to maintain TopN ordering
                    pq.add(term);
                }
                scan.close();

                // Insert all of the IndexTerms into the LinkedHashMap so that you can iterate in TopN ordering
                while (!pq.isEmpty()) {
                    term = pq.poll();
                    hMap.put(term.getTerm(), term);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return hMap;
        }

        /**
         * Deletes the /input and /output directores in the Google Storage bucket on application close
         * so that each time the app is run, input and results from previous executions do not persits.
         */
        protected void cleanUpBucket(){

            Iterator<Blob> iterator;
            Page<Blob> blobs;

            blobs = storage.list(BUCKET_NAME, Storage.BlobListOption.prefix("input"));
            iterator = blobs.iterateAll().iterator();

            System.out.println("Removing contents of the job input directory...");
            while(iterator.hasNext())
                iterator.next().delete();

            blobs = storage.list(BUCKET_NAME, Storage.BlobListOption.prefix("output"));
            iterator = blobs.iterateAll().iterator();

            System.out.println("Removing contents of the job output directory...");
            while(iterator.hasNext())
                iterator.next().delete();

            System.out.println("Bucket cleanup finished!");
        }
    }


    /**
     * Moves the files the user selected from the /assets/ folder in the GCP storage bucket
     * into the /input directory to avoid reuploading the files from the local client on each
     * operation.
     *
     * @param inputPaths
     * @return
     */
    public static boolean moveInputFiles(List<String> inputPaths){
        try {
            for (String p : inputPaths){
                System.out.println("Attempting to move file " + p);
                Blob blob = storage.get(BUCKET_NAME, "assets/" + p);
                CopyWriter CW = blob.copyTo(BUCKET_NAME, "input/" + p);
                Blob copiedBlob = CW.getResult();
//                System.out.printf("Moved file %s into the /input dir the the InvertedIndex job: %s\n", p, copiedBlob.toString());
            }
        } catch (Exception e) {
            //TODO show error on GUI
            System.err.println("Error moving data file into input dir: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
        return true;
    }


    /**
     * Code to merge all of the output files into a new multi-line String
     *
     *  Adapted from:
     *      Source:
     *
     * @param folder the job output folder
     * @throws Exception
     * @source https://github.com/marcusbeacon/Search-Engine/blob/master/SearchEngine/src/main/java/com/mkb90/app/SearchEngine.java
     */
    public static void downloadResults(String folder) throws Exception{
        ArrayList<byte[]> mergeData = new ArrayList<byte[]>();
        int arrayLength = 0;

        Iterator<Blob> iterator;


        // Wait for the InvertedIndex job to finish before downloading results.
        do {
            System.out.println("Output dir is empty or contains intermediate results, checking again in 5 seconds");
            Thread.sleep(5000);

            Page<Blob> blobs = storage.list(BUCKET_NAME, Storage.BlobListOption.prefix(folder));
            iterator = blobs.iterateAll().iterator();

            // NOTE: have to consume the first object, since that is the output/ folder itself, for the back-half of
            //  the while condition to properly detect the completed results
            if(iterator.hasNext())
                System.out.println(iterator.next().getName());
        } while (!iterator.hasNext() || !iterator.next().getName().contains("_SUCCESS"));


        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        while (iterator.hasNext()) {
            Blob blob = iterator.next();
            if (!blob.getName().contains("part-"))
                continue;
            System.out.println("Downloading partial result " + blob.getName());
            blob.downloadTo(byteStream);
            mergeData.add(byteStream.toByteArray());
            arrayLength += byteStream.size();
            byteStream.reset();
        }
        byteStream.close();

        doMerge(mergeData, arrayLength);
    }

    /**
     * Helper function for downloadResults that merges the individual result files into a single String.
     *
     * @param mergeData
     * @param length
     * @throws IOException
     * @source https://github.com/marcusbeacon/Search-Engine/blob/master/SearchEngine/src/main/java/com/mkb90/app/SearchEngine.java
     */
    public static void doMerge(ArrayList<byte[]> mergeData, int length) throws IOException {
        byte[] finalMerge = new byte[length];

        System.out.println("Merging all of the partial results into a single object");

        int destination = 0;
        for (byte[] data: mergeData) {
            System.arraycopy(data, 0, finalMerge, destination, data.length);
            destination += data.length;
        }
        System.out.println("Result merge complete.");
        jobResultStr = new String(finalMerge);
    }


    public static ArrayList<String> stringToList(String s) {
        return new ArrayList<>(Arrays.asList(s.split(" ")));
    }


    /** TODO readme
     *
     *
     * @param projectId
     * @param region
     * @param clusterName
     * @param hadoopFsQuery
     * @throws IOException
     * @throws InterruptedException
     * @source https://github.com/googleapis/java-dataproc/blob/master/samples/snippets/src/main/java/SubmitHadoopFsJob.java
     */
    public static void submitHadoopFsJob(String projectId, String region, String clusterName, String hadoopFsQuery) throws IOException, InterruptedException {
        String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

        // Configure the settings for the job controller client.
        JobControllerSettings jobControllerSettings =
                JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();

        // Create a job controller client with the configured settings. Using a try-with-resources
        // closes the client,
        // but this can also be done manually with the .close() method.
        try (JobControllerClient jobControllerClient =
                     JobControllerClient.create(jobControllerSettings)) {

            // Configure cluster placement for the job.
            JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();

            // Configure Hadoop job settings. The HadoopFS query is set here.
            com.google.cloud.dataproc.v1.HadoopJob hadoopJob =
                    com.google.cloud.dataproc.v1.HadoopJob.newBuilder()
                            .setMainClass("org.apache.hadoop.fs.FsShell")
                            .addAllArgs(stringToList(hadoopFsQuery))
                            .build();

            com.google.cloud.dataproc.v1.Job job = com.google.cloud.dataproc.v1.Job.newBuilder().setPlacement(jobPlacement).setHadoopJob(hadoopJob).build();

            // Submit an asynchronous request to execute the job.
            OperationFuture<com.google.cloud.dataproc.v1.Job, JobMetadata> submitJobAsOperationAsyncRequest =
                    jobControllerClient.submitJobAsOperationAsync(projectId, region, job);

            com.google.cloud.dataproc.v1.Job response = submitJobAsOperationAsyncRequest.get();

            // Print output from Google Cloud Storage.
            Matcher matches =
                    Pattern.compile("gs://(.*?)/(.*)").matcher(response.getDriverOutputResourceUri());
            matches.matches();

            Storage storage = StorageOptions.getDefaultInstance().getService();
            Blob blob = storage.get(matches.group(1), String.format("%s.000000000", matches.group(2)));

            System.out.println(
                    String.format("Job finished successfully: %s", new String(blob.getContent())));

        } catch (ExecutionException e) {
            // If the job does not complete successfully, print the error message.
            System.err.println(String.format("submitHadoopFSJob: %s ", e.getMessage()));
        }
    }


    /**
     * Submits the MapReduce job. Adapted from: https://github.com/googleapis/java-dataproc/blob/master/samples/snippets/src/main/java/SubmitJob.java
     * @param projectId
     * @param region
     * @param clusterName
     * @throws IOException
     * @throws InterruptedException
     */
    public static void submitHadoopMapReduceJob(String projectId, String region, String clusterName) throws IOException, InterruptedException {
        HadoopJob hadoopJob = dataproc.projects().regions().jobs().submit(PROJECT_ID, REGION, new SubmitJobRequest()
                .setJob(new Job().setPlacement(new com.google.api.services.dataproc.model.JobPlacement().setClusterName(CLUSTER_NAME))
                        .setHadoopJob(new HadoopJob().setMainClass("InvertedIndexDriver")
                                .setJarFileUris(ImmutableList.of("gs://" + BUCKET_NAME + "/InvertedIndex.jar"))
                                .setArgs(ImmutableList.of(
                                        "gs://" + JOB_INPUT_DIR, "gs://" + JOB_OUTPUT_DIR)))))
                .execute().getHadoopJob();
        }
}
