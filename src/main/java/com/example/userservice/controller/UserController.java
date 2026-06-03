package com.example.userservice.controller;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.example.userservice.common.ApiResponse;
import com.example.userservice.dto.CreateUserRequest;
import com.example.userservice.dto.UpdateUserRequest;
import com.example.userservice.service.UserService;
import com.example.userservice.vo.UserVO;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/users")
public class UserController {
    private static final Logger log = LoggerFactory.getLogger(UserController.class);

    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @PostMapping
    public ApiResponse<UserVO> create(@Valid @RequestBody CreateUserRequest request) {
        UserVO result = userService.create(request);
        log.info("创建用户成功: id={}", result.getId());
        return ApiResponse.success(result);
    }

    @PutMapping("/{id}")
    public ApiResponse<UserVO> update(
            @PathVariable Long id,
            @Valid @RequestBody UpdateUserRequest request) {
        UserVO result = userService.update(id, request);
        log.info("更新用户成功: id={}", id);
        return ApiResponse.success(result);
    }

    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable Long id) {
        userService.delete(id);
        log.info("删除用户成功: id={}", id);
        return ApiResponse.success(null);
    }

    @GetMapping("/{id}")
    public ApiResponse<UserVO> findById(@PathVariable Long id) {
        UserVO result = userService.findById(id);
        return ApiResponse.success(result);
    }

    @GetMapping
    public ApiResponse<IPage<UserVO>> findByPage(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false) String keyword) {
        IPage<UserVO> result = userService.findByPage(page, size, keyword);
        return ApiResponse.success(result);
    }
}
