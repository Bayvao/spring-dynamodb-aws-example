package com.dynamodb.example.service;

import com.dynamodb.example.config.DynamoDBClientConfig;
import com.dynamodb.example.entity.MovieDetails;
import io.awspring.cloud.dynamodb.DynamoDbTemplate;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.primaryPartitionKey;

@Service
@Slf4j
public class DynamoDBOperationsDemo {

    @Autowired
    DynamoDbTemplate dynamoDbTemplate;

    public MovieDetails saveData(MovieDetails movieDetails) {
        return dynamoDbTemplate.save(movieDetails);
    }

    public MovieDetails loadData(String partitionKey) {
        return dynamoDbTemplate.load(
                Key.builder().partitionValue(partitionKey).build(),
                MovieDetails.class);
    }

    public MovieDetails updateData(MovieDetails movieDetails) {
        return dynamoDbTemplate.update(movieDetails);
    }

    public void deleteDataByObject(MovieDetails movieDetails) {
        dynamoDbTemplate.delete(movieDetails);
    }

    public void deleteDataByKey(String partitionKey) {
        dynamoDbTemplate.delete(
                Key.builder().partitionValue(partitionKey).build(),
                MovieDetails.class
        );
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

    public PageIterable<MovieDetails> scanAllData() {
        return dynamoDbTemplate.scanAll(MovieDetails.class);
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
