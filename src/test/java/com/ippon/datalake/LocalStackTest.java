package com.ippon.datalake;

import cloud.localstack.LocalstackTestRunner;
import cloud.localstack.TestUtils;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.model.FunctionConfiguration;
import com.amazonaws.services.lambda.model.ListFunctionsResult;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;

@RunWith(LocalstackTestRunner.class)
public class LocalStackTest {
    
    @Test
    public void testLocalS3API() {
        AWSLambda lambdaClient = TestUtils.getClientLambda();
        ListFunctionsResult functions = lambdaClient.listFunctions();
        List<FunctionConfiguration> functionList = functions.getFunctions();
        for (FunctionConfiguration function : functionList) {
            System.out.println(function.getFunctionName());
        }
    }
}