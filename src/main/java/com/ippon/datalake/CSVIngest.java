package com.ippon.datalake;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.handlers.TracingHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Implements the RequestHandler interface.
 * This class and the handleCSVIngest method are designed to be the hook
 * for an AWS Lambda function.
 */
public class CSVIngest implements RequestHandler<S3Event, String> {
    
    /**
     * The AWS Lambda environment variable for specifying the allocation of pre-processed files.
     */
    private static final String DESTINATION_PREFIX = "DESTINATION_PREFIX";
    
    /**
     * <p>Parses the incoming raw data and prepares it for ETL pipeline operations.</p>
     *
     * @param bufferedReader <p>The BufferedReader object containing the raw file contents.</p>
     * @return <p>A List of Strings modified in accordance with the methods pre-processing requirements.</p>
     * @throws IOException <p>Thrown in case the BufferedReader.readLine() method throws an exception.</p>
     */
    static List<String> handleCSVIngest(BufferedReader bufferedReader) throws IOException {
        String line;
        List<String> outputCollection = new ArrayList<>();
        while (null != (line = bufferedReader.readLine())) {
            // Remove embedded quotes
            line = line.replaceAll("\"", "");
            
            // Remove commas and unnecessary text from Polygon blocks
            if (line.contains("MULTIPOLYGON")) {
                String polygon = line.substring(line.indexOf("MULTIPOLYGON"), line.indexOf(")))"));
                polygon = polygon.replaceAll("MULTIPOLYGON", "");
                polygon = polygon.replaceAll("\\(\\(\\(", "");
                polygon = polygon.replaceAll("\\)\\)\\)", "").trim();
                polygon = polygon.replaceAll(",\\ ", "::");
                polygon = polygon.replaceAll("\\ ", ":");
                polygon = polygon.replaceAll("\\( \\)", ":::");
                line = line.replaceAll("MULTIPOLYGON ", "").trim();
                line = line.replaceAll("\\(\\(\\([0-9\\-\\.\\,\\ \\(\\)]+\\)\\)\\)", polygon);
            }
            outputCollection.add(line);
        }
        bufferedReader.close();
        return outputCollection;
    }
    
    /**
     * <p>This function is triggered by Lambda on an S3 file creation.</p>
     * <p>At the time of this writing, this function's intended use is to grab
     * newly created files in an S3 bucket's <b>raw</b> directory with the <b>.csv</b>
     * extension and modify the file's contents.  The modified contents are designed
     * to standardize the data for ETL processing at a later date.  The processed
     * files should be stored in the <b>processed/csv</b> directory, but this can be
     * altered via a Lambda environment variable.</p>
     * <p>This AWS Lambda function helps to isolate data within the lake.</p>
     *
     * @param s3event <p>The S3 event which triggered this lambda function.</p>
     * @param context <p>The AWS Lambda Context object for the current invocation.</p>
     * @return
     */
    @Override
    public String handleRequest(S3Event s3event, Context context) {
        LambdaLogger logger = context.getLogger();
        String returnThis = "Inconclusive";
        
        try {
            S3EventNotification.S3EventNotificationRecord record = s3event.getRecords().get(0);
            
            // Object key may have spaces or unicode non-ASCII characters
            String srcBucket = record.getS3().getBucket().getName();
            String srcKey = record.getS3().getObject().getKey()
                    .replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");
            logger.log("New file uploaded to S3: " + srcBucket + "/" + srcKey);
            
            // Download the image from S3 into a stream
            logger.log("Building S3 client...");
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(System.getenv("AWS_REGION"))
                    .withRequestHandlers(
                            new TracingHandler(AWSXRay.getGlobalRecorder()))
                    .build();
            logger.log("Built S3 Client.");
            
            AWSXRay.beginSubsegment("Pull S3 data file into memory...");
            BufferedReader bufferedReader = null;
            List<Tag> tagSet = null;
            try {
                S3Object s3Object = s3Client.getObject(
                        new GetObjectRequest(srcBucket, srcKey));
                bufferedReader = new BufferedReader(
                        new InputStreamReader(s3Object.getObjectContent()));
                GetObjectTaggingResult taggingResult = s3Client.getObjectTagging(
                        new GetObjectTaggingRequest(srcBucket, srcKey));
                tagSet = taggingResult.getTagSet();b
            } finally {
                AWSXRay.endSubsegment();
            }
            
            AWSXRay.beginSubsegment("Massaging Data File...");
            List<String> massagedOutput = null;
            try {
                massagedOutput = handleCSVIngest(bufferedReader);
                bufferedReader.close();
            } catch (IOException e) {
                AWSXRay.getCurrentSegment().addException(e);
                returnThis = e.getMessage();
                return returnThis;
            } finally {
                AWSXRay.endSubsegment();
            }
            
            // Uploading to S3 destination bucket
            String destinationFile = srcKey.substring(srcKey.lastIndexOf("/") + 1);
            destinationFile = System.getenv(DESTINATION_PREFIX)
                    + "/"
                    + destinationFile;
            logger.log("Writing file: " + srcBucket + "/" + destinationFile);
            s3Client.putObject(srcBucket, destinationFile,
                    massagedOutput
                            .stream()
                            .map(x -> x + "\n")
                            .collect(Collectors.joining()));
            returnThis = "OK";
        } catch (UnsupportedEncodingException uee) {
            returnThis = uee.getMessage();
        }
        
        return returnThis;
    }
}
