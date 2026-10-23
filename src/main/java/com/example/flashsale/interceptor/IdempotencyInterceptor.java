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
    public static final String IDEMPOTENCY_HEADER = "Idempotency-Key";

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return true;
        }

        String key = request.getHeader(IDEMPOTENCY_HEADER);
        if (key == null || key.isBlank()) {
            response.setStatus(HttpStatus.BAD_REQUEST.value());
            response.setContentType("application/json;charset=UTF-8");
            response.getWriter().write("{\"message\": \"Header 'Idempotency-Key' là bắt buộc đối với giao dịch này.\"}");
            return false;
        }

        String redisKey = "idempotency:" + key;
        // SETNX: Chỉ tạo nếu key chưa có, khóa tạm thời trong 120s
        Boolean success = redisTemplate.opsForValue().setIfAbsent(redisKey, "PROCESSING", Duration.ofSeconds(120));

        if (Boolean.FALSE.equals(success)) {
            String currentVal = redisTemplate.opsForValue().get(redisKey);
            response.setStatus(HttpStatus.CONFLICT.value());
            response.setContentType("application/json;charset=UTF-8");
            if ("ACCEPTED".equals(currentVal) || "SUCCESS".equals(currentVal)) {
                response.getWriter().write("{\"message\": \"Giao dịch này đã được tiếp nhận hoặc đã hoàn tất trước đó.\", \"status\": \"" + currentVal + "\"}");
            } else {
                response.getWriter().write("{\"message\": \"Request trùng lặp: Đang xử lý giao dịch, vui lòng chờ.\"}");
            }
            return false;
        }

        return true;
    }
}
