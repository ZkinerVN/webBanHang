package com.example.flashsale.interceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Collections;
import java.util.UUID;

@Component
@RequiredArgsConstructor
public class RateLimitInterceptor implements HandlerInterceptor {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> rateLimitScript;

    private static final int MAX_REQUESTS = 5;
    private static final long WINDOW_MS = 10000; // 10 giây

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String userId = request.getParameter("userId");
        String clientKey = (userId != null && !userId.isBlank()) ? "user:" + userId : "ip:" + getClientIp(request);
        String redisKey = "rate_limit:" + clientKey;

        long now = System.currentTimeMillis();
        String requestId = UUID.randomUUID().toString();

        Long allowed = redisTemplate.execute(
                rateLimitScript,
                Collections.singletonList(redisKey),
                String.valueOf(now),
                String.valueOf(WINDOW_MS),
                String.valueOf(MAX_REQUESTS),
                requestId
        );

        if (allowed == null || allowed == 0) {
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value()); // 429 Too Many Requests
            response.setContentType("application/json;charset=UTF-8");
            response.getWriter().write("{\"message\": \"Bạn thao tác quá nhanh! Vui lòng thử lại sau vài giây.\"}");
            return false;
        }

        return true;
    }

    private String getClientIp(HttpServletRequest request) {
        String xfHeader = request.getHeader("X-Forwarded-For");
        if (xfHeader != null && !xfHeader.isBlank()) {
            return xfHeader.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
