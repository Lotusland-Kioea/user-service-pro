package com.example.demo.entity;

import java.io.Serializable;
import java.time.LocalDateTime;

public class User implements Serializable {
    private Long id;
    private String name;
    private String email;
    private LocalDateTime createdAt;

    public User() {}

    public User(String name, String email) {
        this.name = name;
        this.email = email;
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }

    @Override
    public String toString() {
        return "User{id=" + id + ", name='" + name + "', email='" + email + "', createdAt=" + createdAt + '}';
    }
}