/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.ippon.datalake;

import com.amazonaws.services.s3.model.S3Event;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.io.Resources;
import org.junit.Test;

import java.io.*;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Simple Proof of Concept test against sample data,
 * ensuring my pre-processing script performs the steps it should.
 */
public class CSVIngestTest {
    
    @Test
    public void testHandleCSVIngest() throws IOException, URISyntaxException {
        System.setProperty("AWS_REGION", "us-east-2");
        List<String> massagedOutput = null;
        massagedOutput = CSVIngest.handleCSVIngest(
                new BufferedReader(
                        new FileReader(
                                new File(Resources.getResource("testData.csv").toURI()))));
        String output = massagedOutput.stream().map(x -> x + "\n").collect(Collectors.joining());
        System.out.println(output);
    }

    @Test
    public void testSchemaValidation() throws IOException, CSVIngest.SchemaValidationException {
        System.setProperty("AWS_REGION", "us-east-2");
        CSVIngest ingest = new çCSVIngest();
        S3Object testData = new S3Object();
        S3Object testSchema = new S3Object();
        testData.setObjectContent(new FileInputStream("testData.csv"));
        testSchema.setObjectContent(new FileInputStream("testSchema.csv"));
        System.out.println(ingest.validateFileWithSchema(testData, testSchema));
    }
}
