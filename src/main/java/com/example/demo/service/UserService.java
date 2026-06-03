package com.example.demo.service;

import com.example.demo.entity.User;
import com.example.demo.mapper.UserMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserService {

    private static final Logger log = LoggerFactory.getLogger(UserService.class);
    private final UserMapper userMapper;

    public UserService(UserMapper userMapper) {
        this.userMapper = userMapper;
    }

    @CacheEvict(value = "users:all", allEntries = true)
    public User create(User user) {
        System.out.println("[Service] 写入 MySQL: " + user.getName());
        userMapper.insert(user);
        System.out.println("[Service] 删除 Redis 缓存: users:all");
        return user;
    }

    @Cacheable(value = "users:all", unless = "#result == null || #result.isEmpty()")
    public List<User> findAll() {
        System.out.println("[Service] ⚠ 缓存未命中，查询 MySQL...");
        List<User> result = userMapper.findAll();
        System.out.println("[Service] MySQL 返回 " + result.size() + " 条, 写入 Redis (TTL=60s)");
        return result;
    }
}