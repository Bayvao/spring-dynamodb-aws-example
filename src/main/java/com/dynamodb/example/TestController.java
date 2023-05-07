package com.dynamodb.example;

import com.dynamodb.example.entity.MovieDetails;
import com.dynamodb.example.service.DynamoDbOperationDemo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class TestController {

    @Autowired
    private DynamoDbOperationDemo dynamoDbOperationDemo;

}
