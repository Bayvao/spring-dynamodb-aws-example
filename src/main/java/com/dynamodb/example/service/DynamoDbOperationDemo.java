package com.dynamodb.example.service;

import com.dynamodb.example.entity.MovieDetails;
import io.awspring.cloud.dynamodb.DynamoDbTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@Service
public class DynamoDbOperationDemo {

    private DynamoDbTemplate dynamoDbTemplate;
    private DynamoDbTable<MovieDetails> movieTable;

    private DynamoDbClient dynamoDbClient;

    private DynamoDbEnhancedClient dynamoDbEnhancedClient;

    @Autowired
    public DynamoDbOperationDemo(@Qualifier("dynamoDbClient") DynamoDbClient dynamoDbClient,
                                 @Qualifier("dynamoDbEnhancedClient") DynamoDbEnhancedClient dynamoDbEnhancedClient) {

        this.dynamoDbClient = dynamoDbClient;
        this.dynamoDbEnhancedClient = dynamoDbEnhancedClient;

        dynamoDbTemplate = new DynamoDbTemplate(this.dynamoDbEnhancedClient);
        movieTable = DynamoDbEnhancedClient.builder().dynamoDbClient(this.dynamoDbClient).build().table("movie_details",
                TableSchema.fromBean(MovieDetails.class));

    }

    public MovieDetails saveData(MovieDetails movieDetails) {
        return dynamoDbTemplate.save(movieDetails);
    }

    public MovieDetails updateData(MovieDetails movieDetails) {
        return dynamoDbTemplate.update(movieDetails);
    }

    public void deleteByObject(MovieDetails movieDetails) {
        dynamoDbTemplate.delete(movieDetails);
    }

    public MovieDetails findById(String hashKey) {

        Key key = Key.builder().partitionValue(hashKey).build();

        return dynamoDbTemplate.load(key, MovieDetails.class);
    }

    public PageIterable<MovieDetails> scanDataByGenre(String genre) {
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":val1", AttributeValue.fromS(genre));

        Expression filterExpression = Expression.builder()
                .expression("genre = :val1")
                .expressionValues(expressionValues)
                .build();

        ScanEnhancedRequest scanEnhancedRequest = ScanEnhancedRequest.builder()
                .filterExpression(filterExpression).build();

        return dynamoDbTemplate.scan(scanEnhancedRequest, MovieDetails.class);
    }

    public PageIterable<MovieDetails> queryData(String partitionKey,  String genre) {

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":value", AttributeValue.fromS(genre));

        Expression filterExpression = Expression.builder()
                .expression("genre = :val1")
                .expressionValues(expressionValues)
                .build();

        QueryConditional queryConditional = QueryConditional
                .keyEqualTo(
                        Key.builder()
                                .partitionValue(partitionKey)
                                .build());

        QueryEnhancedRequest queryRequest = QueryEnhancedRequest.builder()
                .queryConditional(queryConditional)
                .filterExpression(filterExpression)
                .build();

        return dynamoDbTemplate.query(queryRequest,MovieDetails.class);
    }
}
