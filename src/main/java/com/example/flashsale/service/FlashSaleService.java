package com.example.flashsale.service;

import com.example.flashsale.config.RabbitMQConfig;
import com.example.flashsale.dto.OrderMessage;
import com.example.flashsale.entity.Order;
import com.example.flashsale.entity.OrderStatus;
import com.example.flashsale.repository.OrderRepository;
import com.example.flashsale.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;

@Service
@RequiredArgsConstructor
@Slf4j
public class FlashSaleService {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> stockDecrementScript;
    private final RabbitTemplate rabbitTemplate;
    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;

    public boolean processOrderAsync(OrderMessage message) {
        String stockKey = "product:stock:" + message.productId();

        Long result = redisTemplate.execute(
                stockDecrementScript,
                Collections.singletonList(stockKey),
                String.valueOf(message.quantity())
        );

        if (result == null || result == 0) {
            return false;
        }
        if (result == -1) {
            throw new IllegalStateException("Sản phẩm chưa được warm cache!");
        }

        try {
            // 1. Gửi vào Queue tạo đơn
            rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY, message);
            // 2. Gửi vào Delay Queue để đếm ngược 30 giây chờ thanh toán
            rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.DELAY_ROUTING_KEY, message);

            redisTemplate.opsForValue().set("order:status:" + message.idempotencyKey(), "PENDING", Duration.ofMinutes(10));
            return true;
        } catch (Exception e) {
            log.error("Lỗi gửi RabbitMQ, hoàn kho Redis: {}", e.getMessage());
            redisTemplate.opsForValue().increment(stockKey, message.quantity());
            return false;
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public void saveOrderAndDecrementDbStock(OrderMessage message) {
        int updatedRows = productRepository.decrementStock(message.productId(), message.quantity());
        if (updatedRows == 0) {
            throw new IllegalStateException("Kho PostgreSQL đã hết!");
        }

        Order order = Order.builder()
                .idempotencyKey(message.idempotencyKey())
                .userId(message.userId())
                .productId(message.productId())
                .quantity(message.quantity())
                .status(OrderStatus.PENDING) // Giữ vé chờ thanh toán
                .createdAt(LocalDateTime.now())
                .build();

        orderRepository.saveAndFlush(order);
    }

    // Xác nhận thanh toán thành công
    @Transactional(rollbackFor = Exception.class)
    public boolean confirmPayment(String idempotencyKey) {
        Order order = orderRepository.findByIdempotencyKey(idempotencyKey).orElse(null);
        if (order == null || order.getStatus() != OrderStatus.PENDING) {
            return false;
        }
        order.setStatus(OrderStatus.SUCCESS);
        orderRepository.save(order);
        redisTemplate.opsForValue().set("order:status:" + idempotencyKey, "SUCCESS", Duration.ofMinutes(10));
        return true;
    }

    // Tự động thu hồi vé nếu quá hạn 30s chưa thanh toán
    @Transactional(rollbackFor = Exception.class)
    public void expireUnpaidOrder(OrderMessage message) {
        orderRepository.findByIdempotencyKey(message.idempotencyKey()).ifPresent(order -> {
            if (order.getStatus() == OrderStatus.PENDING) {
                log.warn("Đơn hàng [{}] quá hạn thanh toán! Đang thu hồi {} vé về kho...",
                        message.idempotencyKey(), message.quantity());

                order.setStatus(OrderStatus.EXPIRED);
                orderRepository.save(order);

                // Hoàn kho DB & Redis
                productRepository.incrementStock(order.getProductId(), order.getQuantity());
                redisTemplate.opsForValue().increment("product:stock:" + order.getProductId(), order.getQuantity());
                redisTemplate.opsForValue().set("order:status:" + message.idempotencyKey(), "EXPIRED", Duration.ofMinutes(10));
            }
        });
    }
}