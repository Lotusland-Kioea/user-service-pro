package com.example.userservice.common;

public final class CacheConstants {

    public static final String USERS_PAGE = "users:page";
    public static final String USER_BY_ID = "user:id";

    private CacheConstants() {
        throw new UnsupportedOperationException("Utility class cannot be instantiated");
    }
}
