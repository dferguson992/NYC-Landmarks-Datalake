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

import java.io.*;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

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
    private static final String USE_XRAY = "USE_XRAY";
    private static boolean useXray;
    private static LambdaLogger logger;
    private AmazonS3 s3Client;

    private Set<SchemaElement> schemaDefinition = new HashSet<>();
    private String[] headers;

    /**
     * Initialize static S3 Client for Lambda Usage
     */
    {
        s3Client = AmazonS3ClientBuilder.standard()
                        .withRegion(System.getenv("AWS_REGION"))
                        .withRequestHandlers(
                                new TracingHandler(AWSXRay.getGlobalRecorder()))
                        .build();
    }
    
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
            
            line = line.replaceAll("MULTIPOLYGON ", "").trim();
            line = line.replaceAll("\\(\\(\\([0-9\\-\\.\\,\\ \\(\\)]+\\)\\)\\)", "");
            
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

    private S3Object extractSchema(List<Tag> tagSet, String srcBucket) {
        S3Object file = null;
        try {
            Optional<Tag> schemaLocationTag = tagSet.stream()
                    .filter(x -> x.getKey().equals("SCHEMA_LOCATION"))
                    .distinct()
                    .findFirst();
            file = getFile(srcBucket, "schemas/" + schemaLocationTag.get().getValue());
        } catch (NoSuchElementException nsee){

        }
        return file;
    }

    private S3Object getFile(String srcBucket, String key){
        S3Object file;
        if(useXray){
            AWSXRay.beginSubsegment("Send GET request to S3 to retrieve file " + key + " from bucket " + srcBucket + "...");
            try {
                file = s3Client.getObject(
                        new GetObjectRequest(srcBucket, key));
            } finally {
                AWSXRay.endSubsegment();
            }
        } else {
            file = s3Client.getObject(
                    new GetObjectRequest(srcBucket, key));
        }
        return file;
    }

    public boolean validateFileWithSchema(S3Object s3Object, S3Object schemaFile) throws SchemaValidationException{
        boolean returnThis = true;
        if(schemaFile == null ||
                !schemaFile.getObjectMetadata().getContentType().equals("application/json") ||
                schemaFile.getObjectMetadata().getContentLength() == 0)
            returnThis = false;
        else {
            // Read Schema Definition
            JSONParser jsonParser = new JSONParser();
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(schemaFile.getObjectContent()));

            try {
                Object obj = jsonParser.parse(bufferedReader);
                JSONArray schema = (JSONArray) obj;
                schema.forEach( key -> parseSchemaKey( (JSONObject) key ) );
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            } finally {
                try {
                    bufferedReader.close();
                } catch (IOException e) { }
            }

            // Read first line of data file
            try {
                bufferedReader = new BufferedReader(
                        new InputStreamReader(s3Object.getObjectContent()));
                String headerLine = bufferedReader.readLine();
                headers = headerLine.split(",");
            } catch (IOException e){
                e.printStackTrace();
            } finally {
                try {
                    bufferedReader.close();
                } catch (IOException e) { }
            }

            // Confirm the file header matches the specified schema
            if(headers.length != schemaDefinition.size()){
                returnThis = false;
            } else {
                Iterator<SchemaElement> iterator = schemaDefinition.iterator();
                for (int i = 0; i < headers.length && iterator.hasNext(); i++) {
                    String key = iterator.next().key;
                    if (!headers[i].equals(iterator.next().key)){
                        throw new SchemaValidationException("Schema could not be validatedL: " + headers[i] + " does not equal " + key + "");
                    }
                }
            }
        }
        return returnThis;
    }

    private class SchemaElement{
        String key, type, comment;
        boolean partitionKey;
        SchemaElement(String key, String type, boolean partitionKey, String comment){
            this.key = key;
            this.type = type;
            this.partitionKey = partitionKey;
            this.comment = comment;
        }
    }

    private void parseSchemaKey(JSONObject keyDescriptor) {
        String key = (String) keyDescriptor.get("key");
        String type = (String) keyDescriptor.get("type");
        String partitionKey = (String) keyDescriptor.get("partition_key");
        String comment = (String) keyDescriptor.get("comment");
        schemaDefinition.add(new SchemaElement(key, type, Boolean.valueOf(partitionKey), comment));
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
        logger = context.getLogger();
        String returnThis;
        BufferedReader bufferedReader;
        
        try {
            S3EventNotification.S3EventNotificationRecord record = s3event.getRecords().get(0);

            // Get newly uploaded file characteristics
            // Object key may have spaces or unicode non-ASCII characters
            String srcBucket = record.getS3().getBucket().getName();
            String srcKey = record.getS3().getObject().getKey()
                    .replace('+', ' ');
            srcKey = URLDecoder.decode(srcKey, "UTF-8");
            logger.log("New file uploaded to S3: " + srcBucket + "/" + srcKey);

            S3Object s3Object = getFile(srcBucket, srcKey);
            bufferedReader = new BufferedReader(
                    new InputStreamReader(s3Object.getObjectContent()));

            // Get tags and the schema file for this object
            GetObjectTaggingResult taggingResult = s3Client.getObjectTagging(
                    new GetObjectTaggingRequest(srcBucket, srcKey));
            S3Object schemaFile = extractSchema(taggingResult.getTagSet(), srcBucket);

            // Match file with schema if it exists...
            validateFileWithSchema(s3Object, schemaFile);

            // Massage data file
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

            // Determine if X-Ray Segments should be run
            if(System.getenv(USE_XRAY).toLowerCase().equals("true")){
                useXray = true;
            }

            logger.log("Writing file: " + srcBucket + "/" + destinationFile);
            s3Client.putObject(srcBucket, destinationFile,
                    massagedOutput
                            .stream()
                            .map(x -> x + "\n")
                            .collect(Collectors.joining()));
            returnThis = "OK";
        } catch (UnsupportedEncodingException uee) {
            returnThis = uee.getMessage();
        } catch (SchemaValidationException sve) {
            returnThis = sve.getMessage();
        }
        
        return returnThis;
    }

    class SchemaValidationException extends Exception {
        SchemaValidationException(String message) {
            super(message);
        }
    }
}
