package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

@SpringBootApplication
@EnableCaching
public class DemoApplication {
    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
        System.out.println("========================================");
        System.out.println(" user-service 启动完成! ");
        System.out.println(" API: http://localhost:8080/users");
        System.out.println(" MySQL: localhost:3306/mydb");
        System.out.println(" Redis: localhost:6379");
        System.out.println("========================================");
    }
}