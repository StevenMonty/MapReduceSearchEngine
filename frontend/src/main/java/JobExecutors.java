import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeTokenRequest;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleTokenResponse;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.Dataproc;
import com.google.auth.http.HttpTransportFactory;
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
import com.google.common.collect.ImmutableList;

//import com.google.cloud.dataproc.v1.HadoopJob;
//import com.google.cloud.dataproc.v1.Job;
//import com.google.cloud.dataproc.v1.JobControllerClient;
//import com.google.cloud.dataproc.v1.JobControllerSettings;
//import com.google.cloud.dataproc.v1.JobMetadata;
//import com.google.cloud.dataproc.v1.JobPlacement;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.common.collect.Lists;

import javax.swing.*;
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SwingWorker class to start the InvertedIndex job outside of the main
 * SwingEvent Thread to keep the GUI responsive.
 */

public class JobExecutors {

    static String GCP_AUTH_PATH;
    static Storage storage;
    static String outputData;
    static String BUCKET_NAME;
    static String PROJECT_ID;
    static String REGION;
    static String CLUSTER_NAME;
    static String BUCKET_ASSET_PATH;
    static String JOB_INPUT_DIR;
    static String JOB_OUTPUT_DIR;
    static List<String> inputFiles;
    static Dataproc dataproc;

    static class InvertedIndexExecutor extends SwingWorker<String, String> {

        public InvertedIndexExecutor(List<String> srcFiles) throws IOException {

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

        // TODO add the expected directory structure to the README


        @Override
        protected String doInBackground() throws Exception {

            System.out.println(GCP_AUTH_PATH);

            try{
                System.out.println("Moving selected input files to the BUCKET/input dir...");
                // move the selected files from the bucket's /assets dir to /input
                moveInputFiles(inputFiles);

                System.out.println("Starting the InvertedIndex MapReduceTask...");
                // run the InvertedIndex job
                submitHadoopMapReduceJob(PROJECT_ID, REGION, CLUSTER_NAME);

                // run HadoopFS job to merge output files -> hadoop fs -getmerge /output/dir/on/hdfs/ /desired/local/output/file.txt

                System.out.println("Downloading and merging the InvertedIndex results...");
                // copy the output file from the bucket to local machine
                downloadResults(JOB_OUTPUT_DIR, ".output.txt"); // TODO write this to a file instead of passing the string around

//            String hadoopFsQuery = String.format("-cat /out/part* > gs://%s/output/indices.txt", BUCKET_NAME);

//        String hadoopFsQuery = "hadoop jar InvertedIndex.jar $(BUCKET)/input /out"; //TODO set main jar from bucket add to readme

//            System.out.println(hadoopFsQuery);
//            submitHadoopFsJob(PROJECT_ID, REGION, CLUSTER_NAME, hadoopFsQuery);

            } catch (Exception e) {
                System.err.println("Exception raised during Hadoop Operations: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
            return outputData;
        }
    }


    /**
     * Moves the files the user selected from the /assets/ folder in the GCP storage bucket
     * into the /input directory to avoid reuploading the files from the local client on each
     * operation.   TODO bucket data setup assumption
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
                System.out.printf("Moved file %s into the /input dir the the InvertedIndex job: %s\n", p, copiedBlob.toString());
            }
        } catch (Exception e) {
            //TODO show error on GUI
            System.err.println("Error moving data file into input dir: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static ArrayList<String> stringToList(String s) {
        return new ArrayList<>(Arrays.asList(s.split(" ")));
    }

    /**
     * Code to merge all of the output FileSplits since the
     *  'hadoop fs -getmerge /output/dir/on/hdfs/ /desired/local/output/file.txt' command would not execute from here.
     *  Adapted from:
     *      Source: https://github.com/marcusbeacon/Search-Engine/blob/master/SearchEngine
     *      /src/main/java/com/mkb90/app/SearchEngine.java
     *
     * @param folder
     * @param outputFile
     * @throws Exception
     */
    public static void downloadResults(String folder, String outputFile) throws Exception{
        ArrayList<byte[]> mergeData = new ArrayList<byte[]>();
        int arrayLength = 0;


        Page<Blob> blobs = storage.list(BUCKET_NAME, Storage.BlobListOption.prefix(folder));
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        Iterator<Blob> iterator = blobs.iterateAll().iterator();
        while (iterator.hasNext()) {
            Blob blob = iterator.next();
            if (blob.getName().contains("temp"))
                throw new IOException();
            System.out.println(blob.getName());
            blob.downloadTo(byteStream);
            mergeData.add(byteStream.toByteArray());
            arrayLength += byteStream.size();
            byteStream.reset();
        }
        byteStream.close();

        doMerge(mergeData, arrayLength, outputFile);
    }

    public static void doMerge(ArrayList<byte[]> mergeData, int length, String outputFile) throws IOException {
        byte[] finalMerge = new byte[length];

        int destination = 0;
        for (byte[] data: mergeData) {
            System.arraycopy(data, 0, finalMerge, destination, data.length);
            destination += data.length;
        }

        outputData = new String(finalMerge);


    }


    /** TODO readme
     * https://github.com/googleapis/java-dataproc/blob/master/samples/snippets/src/main/java/SubmitHadoopFsJob.java
     *
     * @param projectId
     * @param region
     * @param clusterName
     * @param hadoopFsQuery
     * @throws IOException
     * @throws InterruptedException
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


    public static void submitHadoopMapReduceJob(String projectId, String region, String clusterName) throws IOException, InterruptedException {
//        GoogleCredentials credentials = GoogleCredentials.newBuilder();
//        InputStream inputStream = new FileInputStream(GCP_AUTH_PATH);
//        GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream).createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
//        HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
//        String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

//        Dataproc constructInvertedIndexDataProc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(),
//                new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault().createScoped(DataprocScopes.all())))
//                .setApplicationName("InvertedIndex").build();

        dataproc.projects().regions().jobs().submit(PROJECT_ID, REGION, new SubmitJobRequest()
                .setJob(new Job().setPlacement(new com.google.api.services.dataproc.model.JobPlacement().setClusterName(CLUSTER_NAME))
                        .setHadoopJob(new HadoopJob().setMainClass("InvertedIndex")
                                .setJarFileUris(ImmutableList.of(BUCKET_NAME + "/InvertedIndex.jar"))
                                .setArgs(ImmutableList.of(
                                        BUCKET_NAME + "/InputFiles", BUCKET_NAME + "/OutputFiles")))))
                .execute();


//        String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

        // Configure the settings for the job controller client.
//        JobControllerSettings jobControllerSettings =
//                JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();

        // Create a job controller client with the configured settings. Using a try-with-resources
        // closes the client,
        // but this can also be done manually with the .close() method.
//        try (JobControllerClient jobControllerClient =
//                     JobControllerClient.create(jobControllerSettings)) {
//
//            // Configure cluster placement for the job.
//            JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();
//
//            // Configure Hadoop job settings. The HadoopFS query is set here.
//            com.google.cloud.dataproc.v1.HadoopJob hadoopJob =
//                    com.google.cloud.dataproc.v1.HadoopJob.newBuilder()
//                            .setMainClass("InvertedIndex").setJarFileUris(ImmutableList.of(BUCKET_NAME, "/InvertedIndex.jar")).setArgs(
//                    ImmutableList.of(BUCKET_NAME + "/input/", BUCKET_NAME + "/output/"))
//                .build();
//
//            com.google.cloud.dataproc.v1.Job job = com.google.cloud.dataproc.v1.Job.newBuilder().setPlacement(jobPlacement).setHadoopJob(hadoopJob).build();
//
//            // Submit an asynchronous request to execute the job.
//            OperationFuture<com.google.cloud.dataproc.v1.Job, JobMetadata> submitJobAsOperationAsyncRequest =
//                    jobControllerClient.submitJobAsOperationAsync(projectId, region, job);
//
//            com.google.cloud.dataproc.v1.Job response = submitJobAsOperationAsyncRequest.get();


//
//
//
//            Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault((HttpTransportFactory) requestInitializer))).setApplicationName("CS1660_SearchEngine").build();
//                Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), requestInitializer)
//                .setApplicationName("InvertedIndex")
//                .build();
//
//        Job submittedJob = dataproc.projects().regions().jobs().submit(projectId, region, new SubmitJobRequest().setJob(
//                           new Job().setPlacement(new com.google.api.services.dataproc.model.JobPlacement().setClusterName(clusterName))
//                                .setHadoopJob(new HadoopJob().setMainClass("InvertedIndex").setJarFileUris(
//                                        ImmutableList.of(BUCKET_NAME + "/InvertedIndex.jar")).setArgs(
//                                                ImmutableList.of(BUCKET_NAME + "/input/", BUCKET_NAME + "/output/")))))
//                .execute();

    }


//    /**
//     * SwingWorker class to perform FileSystem operations between the
//     * Cluster Storage Bucket and the Hadoop Distributed FileSystem
//     */
//    class CloudBucketFSExecutor extends SwingWorker<Void, Void> {
//
//        private final fsOperation op;
//
//        public CloudBucketFSExecutor(fsOperation op) {
//            this.op = op;
//        }
//
//        @Override
//        protected Void doInBackground() throws Exception {
//            return null;
//        }
//
//    }
}
