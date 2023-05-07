package com.dynamodb.example;

import com.dynamodb.example.entity.MovieDetails;
import com.dynamodb.example.service.DynamoDbOperationDemo;
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
//
//    private static DynamoDbTable<MovieDetails> dynamoDbTable;
//    private static DynamoDbTemplate dynamoDbTemplate;

    @Autowired
    private DynamoDbOperationDemo operationDemo;

    @Test
    void contextLoads() {
    }

    /*
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

     */

    @Test
    void testCrud() {
        MovieDetails movieDetails = new MovieDetails("MOV001", "Avengers", null, "Action", "US", "175", "English");
        MovieDetails savedMovie =  operationDemo.saveData(movieDetails);

        Assertions.assertEquals(movieDetails.getId(), savedMovie.getId());

        savedMovie.setTitle("Avengers Endgame");
        MovieDetails updatedMovie = operationDemo.updateData(movieDetails);

        Assertions.assertEquals(movieDetails.getId(), updatedMovie.getId());
        Assertions.assertEquals(savedMovie.getTitle(), updatedMovie.getTitle());

        operationDemo.deleteByObject(updatedMovie);

        MovieDetails fetchedDetails = operationDemo.findById(movieDetails.getId());

        Assertions.assertNull(fetchedDetails);
    }

    @Test
    void testScan() throws InterruptedException {

        MovieDetails actionMovie = new MovieDetails("MOV001", "Avengers", null,
                "Action", "US", "175", "English");
        operationDemo.saveData(actionMovie);

        MovieDetails ThrillerMovie = new MovieDetails("MOV002", "James Bond", null,
                "Thriller", "US", "167", "English");
        operationDemo.saveData(ThrillerMovie);


        PageIterable<MovieDetails> fetchedDataList = operationDemo.scanDataByGenre("Thriller");
        Long countResult = fetchedDataList.stream().count();

        Assertions.assertEquals(1, countResult);

        PageIterable<MovieDetails> fetchedActionDataList = operationDemo.scanDataByGenre("Action");
        Long countActionResult = fetchedActionDataList.stream().count();

        Assertions.assertEquals(1, countActionResult);

       // cleanUp(dynamoDbTable, actionMovie.getId());
        operationDemo.deleteByObject(actionMovie);
        operationDemo.deleteByObject(ThrillerMovie);

    }

/*
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
*/
}
