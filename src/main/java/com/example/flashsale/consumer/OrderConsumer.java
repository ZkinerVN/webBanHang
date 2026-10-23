package com.example.flashsale.consumer;

import com.example.flashsale.config.RabbitMQConfig;
import com.example.flashsale.dto.OrderMessage;
import com.example.flashsale.service.FlashSaleService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderConsumer {

    private final FlashSaleService flashSaleService;
    private final StringRedisTemplate redisTemplate;

    @RabbitListener(queues = RabbitMQConfig.QUEUE_NAME)
    public void handleOrderMessage(OrderMessage message) {
        String statusKey = "order:status:" + message.idempotencyKey();
        String stockKey = "product:stock:" + message.productId();

        try {
            flashSaleService.saveOrderAndDecrementDbStock(message);
            log.info(">> Đã giữ vé thành công (PENDING) cho đơn [{}]", message.idempotencyKey());
        } catch (DataIntegrityViolationException e) {
            log.warn("Bỏ qua message trùng lặp: {}", message.idempotencyKey());
        } catch (Exception e) {
            log.error("Lỗi ghi DB đơn [{}], hoàn kho Redis và đẩy vào DLQ: {}", message.idempotencyKey(), e.getMessage());
            redisTemplate.opsForValue().increment(stockKey, message.quantity());
            redisTemplate.opsForValue().set(statusKey, "FAILED", Duration.ofMinutes(10));
            // Ném ngoại lệ này để RabbitMQ chuyển thẳng message sang Dead Letter Queue (DLQ)
            throw new AmqpRejectAndDontRequeueException("DB Error - Moved to DLQ", e);
        }
    }

    // Worker xử lý khi hết 30 giây giữ chỗ
    @RabbitListener(queues = RabbitMQConfig.TIMEOUT_QUEUE)
    public void handleOrderTimeout(OrderMessage message) {
        flashSaleService.expireUnpaidOrder(message);
    }

    // Worker giám sát Dead Letter Queue
    @RabbitListener(queues = RabbitMQConfig.DLQ_NAME)
    public void handleDeadLetterQueue(OrderMessage failedMessage) {
        log.error("[CẢNH BÁO DLQ] Đơn hàng lỗi cần kiểm tra: {}", failedMessage);
    }
}