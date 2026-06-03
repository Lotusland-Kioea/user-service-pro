package com.example.userservice.config;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import org.apache.ibatis.reflection.MetaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;

@Configuration
public class MyBatisPlusConfig {

    /**
     * 分页插件
     */
    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        interceptor.addInnerInterceptor(new PaginationInnerInterceptor(DbType.MYSQL));
        return interceptor;
    }

    /**
     * 自动填充处理器：create 时填充 createdAt/updatedAt，update 时填充 updatedAt
     */
    @Bean
    public MetaObjectHandler metaObjectHandler() {
        return new MetaObjectHandler() {
            private static final Logger log = LoggerFactory.getLogger(MetaObjectHandler.class);

            @Override
            public void insertFill(MetaObject metaObject) {
                LocalDateTime now = LocalDateTime.now();
                this.strictInsertFill(metaObject, "createdAt", LocalDateTime.class, now);
                this.strictInsertFill(metaObject, "updatedAt", LocalDateTime.class, now);
                log.debug("自动填充 insert: createdAt={}, updatedAt={}", now, now);
            }

            @Override
            public void updateFill(MetaObject metaObject) {
                LocalDateTime now = LocalDateTime.now();
                this.setFieldValByName("updatedAt", now, metaObject);
                log.debug("自动填充 update: updatedAt={}", now);
            }
        };
    }
}
