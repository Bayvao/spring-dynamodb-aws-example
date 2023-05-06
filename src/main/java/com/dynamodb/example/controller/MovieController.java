package com.dynamodb.example.controller;

import com.dynamodb.example.entity.MovieDetails;
import com.dynamodb.example.service.DynamoDBOperationsDemo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MovieController {

    @Autowired
    private DynamoDBOperationsDemo dbOperationsDemo;

    @GetMapping("/hello")
    public MovieDetails saveData() {
        MovieDetails actionMovie = new MovieDetails("MOV001", "Avengers", null, "Action", "US", 175, "English");
        return dbOperationsDemo.saveData(actionMovie);
    }
}
