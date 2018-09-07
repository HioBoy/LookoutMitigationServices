package com.amazon.lookout.utils;

import org.mockito.Mockito;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.UpdateContinuousBackupsRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateContinuousBackupsResult;

public class DynamoDBLocalMocks {

    public static AmazonDynamoDBClient setupSpyDdbClient(AmazonDynamoDBClient dynamoDBClient) {
        dynamoDBClient = Mockito.spy(dynamoDBClient);
        UpdateContinuousBackupsRequest cbRequest = new UpdateContinuousBackupsRequest();
        UpdateContinuousBackupsResult cbrResult = new UpdateContinuousBackupsResult();
        
        Mockito.doReturn(cbrResult).when(dynamoDBClient).updateContinuousBackups(Mockito.any(UpdateContinuousBackupsRequest.class));

        return dynamoDBClient;
    }

    public static AmazonDynamoDB setupSpyDdbClient(AmazonDynamoDB dynamoDBClient) {
        dynamoDBClient = Mockito.spy(dynamoDBClient);
        UpdateContinuousBackupsRequest cbRequest = new UpdateContinuousBackupsRequest();
        UpdateContinuousBackupsResult cbrResult = new UpdateContinuousBackupsResult();
        
        Mockito.doReturn(cbrResult).when(dynamoDBClient).updateContinuousBackups(Mockito.any(UpdateContinuousBackupsRequest.class));

        return dynamoDBClient;
    }

    public static void verifySpyDdbClient(AmazonDynamoDBClient dynamoDBClient) {
        // verify that the updateContinuousBackups method was called
        Mockito.verify(dynamoDBClient).updateContinuousBackups(Mockito.any(UpdateContinuousBackupsRequest.class));
    }
}
