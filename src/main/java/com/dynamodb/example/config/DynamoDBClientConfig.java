package com.dynamodb.example.config;

import com.dynamodb.example.entity.MovieDetails;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;
import java.time.LocalDate;

import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.primaryPartitionKey;

@Configuration
public class DynamoDBClientConfig {

    @Value("${aws.dynamodb.accessKey}")
    private String accessKey;

    @Value("${aws.dynamodb.secretKey}")
    private String secretKey;

    @Value("${aws.dynamodb.endpoint}")
    private String endpoint;

    @Bean
    public DynamoDbClient getDynamoDbClient() {
        return DynamoDbClient.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.AP_SOUTH_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
    }

    @Bean
    public DynamoDbEnhancedClient getDynamoDbEnhancedClient() {
        return DynamoDbEnhancedClient.builder()
                .dynamoDbClient(getDynamoDbClient())
                .build();
    }

    @Bean
    public StaticTableSchema<MovieDetails> dynamoDbTableSchemaResolver(){
        return StaticTableSchema.builder(MovieDetails.class)
                .newItemSupplier(MovieDetails::new)
                .addAttribute(String.class, a -> a.name("id")
                        .getter(MovieDetails::getId)
                        .setter(MovieDetails::setId)
                        .tags(primaryPartitionKey()))
                .addAttribute(String.class, a -> a.name("title")
                        .getter(MovieDetails::getTitle)
                        .setter(MovieDetails::setTitle))
                .addAttribute(String.class, a -> a.name("country")
                        .getter(MovieDetails::getCountry)
                        .setter(MovieDetails::setCountry))
                .addAttribute(String.class, a -> a.name("language")
                        .getter(MovieDetails::getLanguage)
                        .setter(MovieDetails::setLanguage))
                .addAttribute(String.class, a -> a.name("genre")
                        .getter(MovieDetails::getGenre)
                        .setter(MovieDetails::setGenre))
                .addAttribute(Integer.class, a -> a.name("duration")
                        .getter(MovieDetails::getDuration)
                        .setter(MovieDetails::setDuration))
                .addAttribute(LocalDate.class, a -> a.name("release_year")
                        .getter(MovieDetails::getYear)
                        .setter(MovieDetails::setYear))
                .build();

    }
}
