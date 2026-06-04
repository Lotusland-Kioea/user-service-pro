package com.example.userservice.service;

import com.baomidou.mybatisplus.core.metadata.IPage;
import com.example.userservice.dto.CreateUserRequest;
import com.example.userservice.dto.UpdateUserRequest;
import com.example.userservice.entity.User;
import com.example.userservice.event.UserEvent;
import com.example.userservice.exception.BusinessException;
import com.example.userservice.mapper.UserMapper;
import com.example.userservice.service.impl.UserServiceImpl;
import com.example.userservice.vo.UserVO;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserServiceTest {

    @Mock
    private UserMapper userMapper;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    @InjectMocks
    private UserServiceImpl userService;

    private User mockUser;
    private CreateUserRequest createRequest;

    @BeforeEach
    void setUp() {
        mockUser = new User();
        mockUser.setId(1L);
        mockUser.setName("张三");
        mockUser.setEmail("zhangsan@example.com");
        mockUser.setCreatedAt(LocalDateTime.now());
        mockUser.setUpdatedAt(LocalDateTime.now());

        createRequest = new CreateUserRequest();
        createRequest.setName("张三");
        createRequest.setEmail("zhangsan@example.com");
    }

    @Test
    @DisplayName("创建用户 - 成功")
    void testCreateUser_Success() {
        when(userMapper.insert(any(User.class))).thenReturn(1);

        UserVO result = userService.create(createRequest);

        assertNotNull(result);
        assertEquals("张三", result.getName());
        assertEquals("zhangsan@example.com", result.getEmail());
        verify(userMapper, times(1)).insert(any(User.class));
        verify(eventPublisher, times(1)).publishEvent(any(UserEvent.class));
    }

    @Test
    @DisplayName("根据ID查询用户 - 存在")
    void testFindById_Found() {
        when(userMapper.selectById(1L)).thenReturn(mockUser);

        UserVO result = userService.findById(1L);

        assertNotNull(result);
        assertEquals(1L, result.getId());
        assertEquals("张三", result.getName());
    }

    @Test
    @DisplayName("根据ID查询用户 - 不存在抛出异常")
    void testFindById_NotFound() {
        when(userMapper.selectById(999L)).thenReturn(null);

        BusinessException ex = assertThrows(BusinessException.class, () -> userService.findById(999L));

        assertEquals(404, ex.getCode());
        assertEquals("用户不存在", ex.getMessage());
    }

    @Test
    @DisplayName("更新用户 - 成功")
    void testUpdateUser_Success() {
        UpdateUserRequest updateRequest = new UpdateUserRequest();
        updateRequest.setName("李四");
        updateRequest.setEmail("lisi@example.com");

        when(userMapper.selectById(1L)).thenReturn(mockUser);
        when(userMapper.updateById(any(User.class))).thenReturn(1);

        UserVO result = userService.update(1L, updateRequest);

        assertNotNull(result);
        verify(userMapper, times(1)).updateById(any(User.class));
        verify(eventPublisher, times(1)).publishEvent(any(UserEvent.class));
    }

    @Test
    @DisplayName("更新用户 - 不存在")
    void testUpdateUser_NotFound() {
        UpdateUserRequest updateRequest = new UpdateUserRequest();
        updateRequest.setName("李四");
        updateRequest.setEmail("lisi@example.com");

        when(userMapper.selectById(999L)).thenReturn(null);

        assertThrows(BusinessException.class, () -> userService.update(999L, updateRequest));
    }

    @Test
    @DisplayName("删除用户 - 成功")
    void testDeleteUser_Success() {
        when(userMapper.selectById(1L)).thenReturn(mockUser);
        when(userMapper.deleteById(1L)).thenReturn(1);

        assertDoesNotThrow(() -> userService.delete(1L));

        verify(userMapper, times(1)).deleteById(1L);
        verify(eventPublisher, times(1)).publishEvent(any(UserEvent.class));
    }

    @Test
    @DisplayName("删除用户 - 不存在")
    void testDeleteUser_NotFound() {
        when(userMapper.selectById(999L)).thenReturn(null);

        assertThrows(BusinessException.class, () -> userService.delete(999L));

        verify(userMapper, never()).deleteById(anyLong());
    }
}
