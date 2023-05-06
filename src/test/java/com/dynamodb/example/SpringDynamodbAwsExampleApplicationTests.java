package com.dynamodb.example;

import com.dynamodb.example.entity.MovieDetails;
import io.awspring.cloud.dynamodb.DynamoDbTemplate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.ScanEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.util.*;

@SpringBootTest
class SpringDynamodbAwsExampleApplicationTests {

    private static DynamoDbTable<MovieDetails> dynamoDbTable;
    private static DynamoDbTemplate dynamoDbTemplate;

    @Test
    void contextLoads() {
    }

    @BeforeAll
    public static void createTable() {
        DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://localhost:8000")).region(Region.AP_SOUTH_1)
                .credentialsProvider(StaticCredentialsProvider
                        .create(AwsBasicCredentials.create("accesskey", "secretKey")))
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamoDbClient).build();

        dynamoDbTemplate = new DynamoDbTemplate(enhancedClient);
        dynamoDbTable = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamoDbClient).build().table("movie_details",
                TableSchema.fromBean(MovieDetails.class));
        describeAndCreateTable(dynamoDbClient, null);

    }

    @Test
    @MethodSource("argumentSource")
    void testCrud() {
        MovieDetails movieDetails = new MovieDetails("MOV001", "Avengers", null, "Action", "US", "175", "English");
        MovieDetails savedMovie =  dynamoDbTemplate.save(movieDetails);

        Assertions.assertEquals(movieDetails.getId(), savedMovie.getId());

        savedMovie.setTitle("Avengers Endgame");
        MovieDetails updatedMovie = dynamoDbTemplate.update(movieDetails);

        Assertions.assertEquals(movieDetails.getId(), updatedMovie.getId());
        Assertions.assertEquals(savedMovie.getTitle(), updatedMovie.getTitle());

        dynamoDbTemplate.delete(updatedMovie);

        MovieDetails fetchedDetails = dynamoDbTemplate.load(
                Key.builder().partitionValue(movieDetails.getId()).build(),
                MovieDetails.class);

        Assertions.assertNull(fetchedDetails);
    }

    @Test
    @MethodSource("argumentSource")
    void testScan() throws InterruptedException {

        MovieDetails actionMovie = new MovieDetails("MOV001", "Avengers", null,
                "Action", "US", "175", "English");
        dynamoDbTemplate.save(actionMovie);

        MovieDetails ThrillerMovie = new MovieDetails("MOV002", "James Bond", null,
                "Thriller", "US", "167", "English");
        dynamoDbTemplate.save(ThrillerMovie);


        PageIterable<MovieDetails> fetchedDataList = scanDataByGenre("Thriller");
        Long countResult = fetchedDataList.stream().count();

        Assertions.assertEquals(1, countResult);

        PageIterable<MovieDetails> fetchedActionDataList = scanDataByGenre("Action");
        Long countActionResult = fetchedActionDataList.stream().count();

        Assertions.assertEquals(1, countActionResult);

        cleanUp(dynamoDbTable, actionMovie.getId());
        dynamoDbTemplate.delete(ThrillerMovie);

    }

    private PageIterable<MovieDetails> scanDataByGenre(String genre) {
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":val1", AttributeValue.fromS(genre));

        Expression filterExpression = Expression.builder()
                .expression("genre = :val1")
                .expressionValues(expressionValues)
                .build();

        ScanEnhancedRequest scanEnhancedRequest = ScanEnhancedRequest.builder()
                .filterExpression(filterExpression).build();

        return dynamoDbTable.scan(scanEnhancedRequest);
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

    private static void describeAndCreateTable(DynamoDbClient dynamoDbClient, @Nullable String tablePrefix) {
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(AttributeDefinition.builder().attributeName("id").attributeType("S").build());

        ArrayList<KeySchemaElement> tableKeySchema = new ArrayList<>();
        tableKeySchema.add(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build());
        List<KeySchemaElement> indexKeySchema = new ArrayList<>();
        indexKeySchema.add(KeySchemaElement.builder().attributeName("id").keyType(KeyType.HASH).build());

        GlobalSecondaryIndex precipIndex = GlobalSecondaryIndex.builder().indexName("movie_info_idx")
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits((long) 10)
                        .writeCapacityUnits((long) 1).build())
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build()).keySchema(indexKeySchema)
                .build();
        String tableName = StringUtils.hasText(tablePrefix) ? tablePrefix.concat("movie_details") : "movie_details";
        CreateTableRequest createTableRequest = CreateTableRequest.builder().tableName(tableName)
                .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits((long) 10)
                        .writeCapacityUnits((long) 1).build())
                .attributeDefinitions(attributeDefinitions).keySchema(tableKeySchema)
                .globalSecondaryIndexes(precipIndex).build();

        try {
            dynamoDbClient.createTable(createTableRequest);
        }
        catch (ResourceInUseException e) {
            // table already exists, do nothing
        }
    }

    public static void cleanUp(DynamoDbTable<MovieDetails> dynamoDbTable, String uuid) {
        dynamoDbTable.deleteItem(Key.builder().partitionValue(uuid).build());
    }

    private static java.util.stream.Stream<Arguments> argumentSource() {
        return java.util.stream.Stream.of(Arguments.of(dynamoDbTable, dynamoDbTemplate));
    }

}
