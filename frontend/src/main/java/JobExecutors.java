import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.Dataproc;
import com.google.cloud.dataproc.v1.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.*;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.common.collect.ImmutableList;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.dataproc.v1.HadoopJob;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobMetadata;
import com.google.cloud.dataproc.v1.JobPlacement;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import javax.swing.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SwingWorker class to start the InvertedIndex job
 */

public class JobExecutors {

    static String GCP_AUTH_PATH = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
    static Storage storage;
    static String outputData;
    static String BUCKET_NAME;
    static String PROJECT_ID;
    static String REGION;
    static String CLUSTER_NAME;
    static String[] inputFiles;
    static Dataproc dataproc;


    static class InvertedIndexExecutor extends SwingWorker<Void, Void> {


        public InvertedIndexExecutor(String[] srcFiles) throws IOException {

            inputFiles = srcFiles;
            BUCKET_NAME = System.getenv("BUCKET_NAME");
            PROJECT_ID = System.getenv("PROJECT_ID");
            REGION = System.getenv("REGION");
            CLUSTER_NAME = System.getenv("CLUSTER_NAME");
            storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
            dataproc  = new Dataproc.Builder(new NetHttpTransport(),
                        new JacksonFactory(),
                        new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault()
                                .createScoped(DataprocScopes.all()))).setApplicationName("CS1660SearchEngine").build();
        }



        @Override
        protected Void doInBackground() throws Exception {
            //TODO:
            // read in paths from srcFiles

            // move the selected files from the bucket's /assets dir to /input
            moveInputFiles(inputFiles);
            // run the InvertedIndex job
            //TODO


            // run HadoopFS job to merge output files -> hadoop fs -getmerge /output/dir/on/hdfs/ /desired/local/output/file.txt

            // copy the output file from the bucket to local machine

            // TODO add the expected directory structure to the README




            String hadoopFsQuery = String.format("-cat /out/part* > gs://%s/output/indices.txt", BUCKET_NAME);

//        String hadoopFsQuery = "hadoop jar InvertedIndex.jar $(BUCKET)/input /out"; //TODO set main jar from bucket add to readme

            System.out.println(hadoopFsQuery);
            submitHadoopFsJob(PROJECT_ID, REGION, CLUSTER_NAME, hadoopFsQuery);
            return null;
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
    public static boolean moveInputFiles(String[] inputPaths){
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
     * Source: https://github.com/marcusbeacon/Search-Engine/blob/master/SearchEngine/src/main/java/com/mkb90/app/SearchEngine.java
     * @param folder
     * @param outputFile
     * @throws Exception
     */
    public static void downloadObject(String folder, String outputFile) throws Exception{
        ArrayList<byte[]> mergeData = new ArrayList<byte[]>();
        int arrayLength = 0;


        Page<Blob> blobs = storage.list(BUCKET_NAME, Storage.BlobListOption.prefix(folder));
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        Iterator<Blob> iterator = blobs.iterateAll().iterator();
        System.out.println(iterator.next());
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
            HadoopJob hadoopJob =
                    HadoopJob.newBuilder()
                            .setMainClass("org.apache.hadoop.fs.FsShell")
                            .addAllArgs(stringToList(hadoopFsQuery))
                            .build();

            Job job = Job.newBuilder().setPlacement(jobPlacement).setHadoopJob(hadoopJob).build();

            // Submit an asynchronous request to execute the job.
            OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest =
                    jobControllerClient.submitJobAsOperationAsync(projectId, region, job);

            Job response = submitJobAsOperationAsyncRequest.get();

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


    public static void submitHadoopMapReduceJob(String projectId, String region, String clusterName, String hadoopFsQuery) throws IOException, InterruptedException {

        dataproc.projects().regions().jobs().submit(projectId, region, new SubmitJobRequest()
                        .setJob(new Job().setPlacement(new JobPlacement().setClusterName(clusterName))
                                .setHadoopJob(new HadoopJob()
                                        .setMainClass("InvertedIndexDriver")
                                        .setJarFileUris(ImmutableList.of("gs://bucket/path/to/your/spark-job.jar"))
                                        .setArgs(ImmutableList.of(
                                                "arg1", "arg2", "arg3")))))
                .execute();


//        String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);
//
//        // Configure the settings for the job controller client.
//        JobControllerSettings jobControllerSettings =
//                JobControllerSettings.newBuilder().setEndpoint(myEndpoint).build();
//
//        // Create a job controller client with the configured settings. Using a try-with-resources
//        // closes the client,
//        // but this can also be done manually with the .close() method.
//        try (JobControllerClient jobControllerClient =
//                     JobControllerClient.create(jobControllerSettings)) {
//
//            // Configure cluster placement for the job.
//            JobPlacement jobPlacement = JobPlacement.newBuilder().setClusterName(clusterName).build();
//
//            // Configure Hadoop job settings.
//            HadoopJob hadoopJob =
//                    HadoopJob.newBuilder()
//                            .setMainClass("InvertedIndexDriver")
//                            .addAllArgs(stringToList(hadoopFsQuery))
//                            .build();
//
//            Job job = Job.newBuilder().setPlacement(jobPlacement).setHadoopJob(hadoopJob).build();
//
//            // Submit an asynchronous request to execute the job.
//            OperationFuture<Job, JobMetadata> submitJobAsOperationAsyncRequest =
//                    jobControllerClient.submitJobAsOperationAsync(projectId, region, job);
//
//            Job response = submitJobAsOperationAsyncRequest.get();
//
//            // Print output from Google Cloud Storage.
//            Matcher matches =
//                    Pattern.compile("gs://(.*?)/(.*)").matcher(response.getDriverOutputResourceUri());
//            matches.matches();
//
//            Storage storage = StorageOptions.getDefaultInstance().getService();
//            Blob blob = storage.get(matches.group(1), String.format("%s.000000000", matches.group(2)));
//
//            System.out.println(
//                    String.format("Job finished successfully: %s", new String(blob.getContent())));
//
//        } catch (ExecutionException e) {
//            // If the job does not complete successfully, print the error message.
//            System.err.println(String.format("submitHadoopFSJob: %s ", e.getMessage()));
//        }
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
