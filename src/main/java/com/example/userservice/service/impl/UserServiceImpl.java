package com.example.userservice.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.example.userservice.common.CacheConstants;
import com.example.userservice.dto.CreateUserRequest;
import com.example.userservice.dto.UpdateUserRequest;
import com.example.userservice.entity.User;
import com.example.userservice.event.UserEvent;
import com.example.userservice.event.UserEventType;
import com.example.userservice.exception.BusinessException;
import com.example.userservice.mapper.UserMapper;
import com.example.userservice.service.UserService;
import com.example.userservice.vo.UserVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

@Service
public class UserServiceImpl implements UserService {

    private static final Logger log = LoggerFactory.getLogger(UserServiceImpl.class);

    private final UserMapper userMapper;
    private final ApplicationEventPublisher eventPublisher;

    public UserServiceImpl(UserMapper userMapper, ApplicationEventPublisher eventPublisher) {
        this.userMapper = userMapper;
        this.eventPublisher = eventPublisher;
    }

    @Override
    @Transactional
    @Caching(
        evict = { @CacheEvict(value = CacheConstants.USERS_PAGE, allEntries = true) },
        put = { @CachePut(value = CacheConstants.USER_BY_ID, key = "#result.id") }
    )
    public UserVO create(CreateUserRequest request) {
        log.info("创建用户: {}", request);
        User user = new User();
        BeanUtils.copyProperties(request, user);
        userMapper.insert(user);
        UserVO result = UserVO.from(user);
        // 发布用户创建事件 → @TransactionalEventListener 转发到 Kafka
        eventPublisher.publishEvent(UserEvent.of(UserEventType.CREATED,
                user.getId(), user.getName(), user.getEmail()));
        log.info("创建用户成功: {}", result);
        return result;
    }

    @Override
    @Transactional
    @Caching(evict = {
        @CacheEvict(value = CacheConstants.USERS_PAGE, allEntries = true),
        @CacheEvict(value = CacheConstants.USER_BY_ID, key = "#id")
    })
    public UserVO update(Long id, UpdateUserRequest request) {
        log.info("更新用户 ID:{}", id);
        User user = userMapper.selectById(id);
        if (user == null) {
            throw new BusinessException(404, "用户不存在");
        }
        BeanUtils.copyProperties(request, user);
        userMapper.updateById(user);
        // 发布用户更新事件 → @TransactionalEventListener 转发到 Kafka
        eventPublisher.publishEvent(UserEvent.of(UserEventType.UPDATED,
                user.getId(), user.getName(), user.getEmail()));
        return UserVO.from(user);
    }

    @Override
    @Transactional
    @Caching(evict = {
        @CacheEvict(value = CacheConstants.USERS_PAGE, allEntries = true),
        @CacheEvict(value = CacheConstants.USER_BY_ID, key = "#id")
    })
    public void delete(Long id) {
        log.info("删除用户 ID:{}", id);
        User user = userMapper.selectById(id);
        if (user == null) {
            throw new BusinessException(404, "用户不存在");
        }
        userMapper.deleteById(id);
        // 发布用户删除事件 → @TransactionalEventListener 转发到 Kafka
        eventPublisher.publishEvent(UserEvent.of(UserEventType.DELETED,
                user.getId(), user.getName(), user.getEmail()));
    }

    @Override
    @Cacheable(value = CacheConstants.USER_BY_ID, key = "#id", unless = "#result == null")
    public UserVO findById(Long id) {
        log.debug("查询用户 ID:{}", id);
        User user = userMapper.selectById(id);
        if (user == null) {
            throw new BusinessException(404, "用户不存在");
        }
        return UserVO.from(user);
    }

    @Override
    @Cacheable(value = CacheConstants.USERS_PAGE,
        key = "#page + ':' + #size + ':' + (#keyword == null ? '' : #keyword)",
        unless = "#result == null || #result.records.isEmpty()")
    public IPage<UserVO> findByPage(int page, int size, String keyword) {
        log.debug("分页查询用户 page={}, size={}, keyword={}", page, size, keyword);
        LambdaQueryWrapper<User> wrapper = new LambdaQueryWrapper<>();
        if (StringUtils.hasText(keyword)) {
            wrapper.and(w -> w.like(User::getName, keyword).or().like(User::getEmail, keyword));
        }
        wrapper.orderByDesc(User::getId);
        Page<User> pageResult = userMapper.selectPage(new Page<>(page, size), wrapper);
        return pageResult.convert(UserVO::from);
    }
}
