package com.ippon.datalake;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
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

public class CSVIngest implements RequestHandler<S3Event, String> {
    
    private static final String DESTINATION_PREFIX = "DESTINATION_PREFIX";
    
    /**
     * Parses the incoming raw data and prepares it for ETL pipeline operations.
     *
     * @param bufferedReader
     * @return
     * @throws IOException
     */
    static List<String> handleCSVIngest(BufferedReader bufferedReader) throws IOException {
        String line;
        List<String> outputCollection = new ArrayList<String>();
        while (null != (line = bufferedReader.readLine())) {
            // Remove embedded quotes
            line = line.replaceAll("\"", "");
            
            // Remove commas and unnecessary text from Polygon blocks
            if (line.contains("MULTIPOLYGON")) {
                String polygon = line.substring(line.indexOf("MULTIPOLYGON ((("), line.indexOf(")))"));
                polygon = polygon.replaceAll("MULTIPOLYGON ", "");
                polygon = polygon.replaceAll("\\(\\(\\(", "");
                polygon = polygon.replaceAll("\\)\\)\\)", "");
                polygon = polygon.replaceAll(",\\ ", "::");
                polygon = polygon.replaceAll("\\ ", ":");
                line = line.replaceAll("MULTIPOLYGON \\(\\(\\([0-9\\-\\.\\,\\ ]+\\)\\)\\)", polygon);
            }
            outputCollection.add(line);
        }
        return outputCollection;
    }
    
    /**
     * This function is triggered by Lambda on an S3 file creation to the raw directory
     * of the specified bucket for any csv file.
     *
     * @param s3event
     * @param context
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
            try {
                S3Object s3Object = s3Client.getObject(
                        new GetObjectRequest(srcBucket, srcKey));
                bufferedReader = new BufferedReader(
                        new InputStreamReader(s3Object.getObjectContent()));
            } finally {
                AWSXRay.endSubsegment();
            }
            
            AWSXRay.beginSubsegment("Massaging Data File...");
            List<String> massagedOutput = null;
            try {
                massagedOutput = handleCSVIngest(bufferedReader);
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
