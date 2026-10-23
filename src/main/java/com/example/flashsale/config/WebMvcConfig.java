package com.example.flashsale.config;

import com.example.flashsale.interceptor.IdempotencyInterceptor;
import com.example.flashsale.interceptor.RateLimitInterceptor;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@RequiredArgsConstructor
public class WebMvcConfig implements WebMvcConfigurer {

    private final RateLimitInterceptor rateLimitInterceptor;
    private final IdempotencyInterceptor idempotencyInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // Chạy Rate Limit trước, sau đó mới kiểm tra Idempotency
        registry.addInterceptor(rateLimitInterceptor)
                .addPathPatterns("/api/v1/flash-sale/purchase");

        registry.addInterceptor(idempotencyInterceptor)
                .addPathPatterns("/api/v1/flash-sale/purchase");
    }
}