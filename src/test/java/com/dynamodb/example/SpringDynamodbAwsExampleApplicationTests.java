package com.dynamodb.example;

import com.dynamodb.example.entity.MovieDetails;
import com.dynamodb.example.service.DynamoDBOperationsDemo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;

import java.time.LocalDate;

@SpringBootTest
class SpringDynamodbAwsExampleApplicationTests {


    @Autowired
    private DynamoDBOperationsDemo dbOperationsDemo;

    @Test
    void contextLoads() {
    }

    @Test
    void testCrud() {
        MovieDetails movieDetails = new MovieDetails("MOV001", "Avengers", null, "Action", "US", 175, "English");
        MovieDetails savedMovie =  dbOperationsDemo.saveData(movieDetails);

        Assertions.assertEquals(movieDetails.getId(), savedMovie.getId());

        savedMovie.setTitle("Avengers Endgame");
        MovieDetails updatedMovie = dbOperationsDemo.updateData(movieDetails);

        Assertions.assertEquals(movieDetails.getId(), updatedMovie.getId());
        Assertions.assertEquals(savedMovie.getTitle(), updatedMovie.getTitle());

        dbOperationsDemo.deleteDataByObject(updatedMovie);

        MovieDetails fetchedDetails = dbOperationsDemo.loadData(movieDetails.getId());

        Assertions.assertNull(fetchedDetails);
    }

    @Test
    void testScan() throws InterruptedException {

        MovieDetails actionMovie = new MovieDetails("MOV001", "Avengers", null, "Action", "US", 175, "English");
        dbOperationsDemo.saveData(actionMovie);

        MovieDetails ThrillerMovie = new MovieDetails("MOV002", "James Bond", null, "Thriller", "US", 167, "English");
        dbOperationsDemo.saveData(ThrillerMovie);

        PageIterable<MovieDetails> fetchedDataList = dbOperationsDemo.scanDataByGenre("Thriller");
        Long countResult = fetchedDataList.stream().count();

        Assertions.assertEquals(1, countResult);

        PageIterable<MovieDetails> fetchedActionDataList = dbOperationsDemo.scanDataByGenre("Action");
        Long countActionResult = fetchedActionDataList.stream().count();

        Assertions.assertEquals(1, countActionResult);

        dbOperationsDemo.deleteDataByKey(actionMovie.getId());
        dbOperationsDemo.deleteDataByObject(ThrillerMovie);

    }

}
