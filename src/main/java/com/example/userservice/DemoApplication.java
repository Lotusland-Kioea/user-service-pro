package com.example.userservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
        System.out.println("========================================");
        System.out.println("  user-service-pro v1.1.0 启动成功!");
        System.out.println("  API:      http://localhost:8080/users");
        System.out.println("  数据库:   localhost:3306/mydb");
        System.out.println("  Redis:    localhost:6379");
        System.out.println("  Kafka:    localhost:9092,9094,9096");
        System.out.println("  KafkaLab: http://localhost:8080/kafka-lab");
        System.out.println("========================================");
    }
}
