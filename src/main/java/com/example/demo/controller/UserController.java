package com.example.demo.controller;

import com.example.demo.entity.User;
import com.example.demo.service.UserService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/users")
public class UserController {

    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @PostMapping
    public User create(@RequestBody User user) {
        System.out.println("[POST /users] 收到创建请求: name=" + user.getName() + ", email=" + user.getEmail());
        User created = userService.create(user);
        System.out.println("[POST /users] 写入成功, id=" + created.getId() + ", 已清除 Redis 缓存");
        return created;
    }

    @GetMapping
    public List<User> findAll() {
        System.out.println("[GET /users] 收到查询请求...");
        List<User> users = userService.findAll();
        System.out.println("[GET /users] 返回 " + users.size() + " 条记录");
        return users;
    }
}