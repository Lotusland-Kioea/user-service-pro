package com.example.userservice.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.example.userservice.dto.CreateUserRequest;
import com.example.userservice.dto.UpdateUserRequest;
import com.example.userservice.vo.UserVO;

public interface UserService {

    UserVO create(CreateUserRequest request);

    UserVO update(Long id, UpdateUserRequest request);

    void delete(Long id);

    UserVO findById(Long id);

    IPage<UserVO> findByPage(int page, int size, String keyword);
}
