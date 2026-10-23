package com.example.flashsale.interceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class IdempotencyInterceptor implements HandlerInterceptor {

    private final StringRedisTemplate redisTemplate;
    private static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return true;
        }

        String key = request.getHeader(IDEMPOTENCY_HEADER);
        if (key == null || key.isBlank()) {
            return true;
        }

        String redisKey = "idempotency:" + key;
        // SETNX: Chỉ tạo nếu key chưa có, khóa tạm thời trong 120s
        Boolean success = redisTemplate.opsForValue().setIfAbsent(redisKey, "PROCESSING", Duration.ofSeconds(120));

        if (Boolean.FALSE.equals(success)) {
            response.setStatus(HttpStatus.CONFLICT.value());
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write("Request trùng lặp: Đang xử lý hoặc đã hoàn tất.");
            return false;
        }

        return true;
    }
}