package com.example.userservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
        System.out.println("========================================");
        System.out.println("  user-service-pro 启动成功!");
        System.out.println("  API 地址: http://localhost:8080/users");
        System.out.println("  数据库:   localhost:3306/mydb");
        System.out.println("  Redis:    localhost:6379");
        System.out.println("========================================");
    }
}
