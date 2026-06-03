package com.example.userservice.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.example.userservice.dto.CreateUserRequest;
import com.example.userservice.dto.UpdateUserRequest;
import com.example.userservice.exception.BusinessException;
import com.example.userservice.exception.GlobalExceptionHandler;
import com.example.userservice.service.UserService;
import com.example.userservice.vo.UserVO;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.LocalDateTime;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(UserController.class)
@Import(GlobalExceptionHandler.class)
class UserControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private UserService userService;

    private UserVO mockUserVO;

    @BeforeEach
    void setUp() {
        mockUserVO = new UserVO();
        mockUserVO.setId(1L);
        mockUserVO.setName("张三");
        mockUserVO.setEmail("zhangsan@example.com");
        mockUserVO.setCreatedAt(LocalDateTime.now());
        mockUserVO.setUpdatedAt(LocalDateTime.now());
    }

    @Test
    @DisplayName("POST /users - 创建用户成功")
    void testCreateUser_Success() throws Exception {
        CreateUserRequest request = new CreateUserRequest();
        request.setName("张三");
        request.setEmail("zhangsan@example.com");

        when(userService.create(any(CreateUserRequest.class))).thenReturn(mockUserVO);

        mockMvc.perform(post("/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.name").value("张三"))
                .andExpect(jsonPath("$.data.email").value("zhangsan@example.com"));
    }

    @Test
    @DisplayName("POST /users - 参数校验失败（空name）")
    void testCreateUser_ValidationFail() throws Exception {
        CreateUserRequest request = new CreateUserRequest();
        request.setName("");
        request.setEmail("test@example.com");

        mockMvc.perform(post("/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(400))
                .andExpect(jsonPath("$.message").isNotEmpty());
    }

    @Test
    @DisplayName("POST /users - 参数校验失败（非法email）")
    void testCreateUser_InvalidEmail() throws Exception {
        CreateUserRequest request = new CreateUserRequest();
        request.setName("张三");
        request.setEmail("not-an-email");

        mockMvc.perform(post("/users")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code").value(400));
    }

    @Test
    @DisplayName("GET /users/{id} - 查询成功")
    void testFindById_Found() throws Exception {
        when(userService.findById(1L)).thenReturn(mockUserVO);

        mockMvc.perform(get("/users/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.id").value(1))
                .andExpect(jsonPath("$.data.name").value("张三"));
    }

    @Test
    @DisplayName("GET /users/{id} - 用户不存在")
    void testFindById_NotFound() throws Exception {
        when(userService.findById(999L)).thenThrow(new BusinessException(404, "用户不存在"));

        mockMvc.perform(get("/users/999"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code").value(404))
                .andExpect(jsonPath("$.message").value("用户不存在"));
    }

    @Test
    @DisplayName("GET /users - 分页查询")
    void testFindByPage() throws Exception {
        Page<UserVO> page = new Page<>(1, 10);
        page.setRecords(List.of(mockUserVO));
        page.setTotal(1);

        when(userService.findByPage(1, 10, null)).thenReturn(page);

        mockMvc.perform(get("/users")
                        .param("page", "1")
                        .param("size", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.records[0].name").value("张三"));
    }

    @Test
    @DisplayName("DELETE /users/{id} - 删除成功")
    void testDelete_Success() throws Exception {
        doNothing().when(userService).delete(1L);

        mockMvc.perform(delete("/users/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200));
    }

    @Test
    @DisplayName("PUT /users/{id} - 更新成功")
    void testUpdate_Success() throws Exception {
        UpdateUserRequest request = new UpdateUserRequest();
        request.setName("李四");
        request.setEmail("lisi@example.com");

        UserVO updatedVO = new UserVO();
        updatedVO.setId(1L);
        updatedVO.setName("李四");
        updatedVO.setEmail("lisi@example.com");
        updatedVO.setCreatedAt(LocalDateTime.now());
        updatedVO.setUpdatedAt(LocalDateTime.now());

        when(userService.update(eq(1L), any(UpdateUserRequest.class))).thenReturn(updatedVO);

        mockMvc.perform(put("/users/1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.name").value("李四"));
    }
}
