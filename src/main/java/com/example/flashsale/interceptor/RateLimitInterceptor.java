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
public class RateLimitInterceptor implements HandlerInterceptor {

    private final StringRedisTemplate redisTemplate;
    private static final int MAX_REQUESTS = 5;
    private static final int WINDOW_SECONDS = 10;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String userId = request.getParameter("userId");
        String clientKey = (userId != null && !userId.isBlank()) ? "user:" + userId : "ip:" + request.getRemoteAddr();
        String redisKey = "rate_limit:" + clientKey;

        Long count = redisTemplate.opsForValue().increment(redisKey);
        if (count != null && count == 1) {
            redisTemplate.expire(redisKey, Duration.ofSeconds(WINDOW_SECONDS));
        }

        if (count != null && count > MAX_REQUESTS) {
            response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value()); // 429 Too Many Requests
            response.setCharacterEncoding("UTF-8");
            response.getWriter().write("Bạn thao tác quá nhanh! Vui lòng thử lại sau vài giây.");
            return false;
        }
        return true;
    }
}