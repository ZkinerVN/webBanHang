package com.example.flashsale.service;

import com.example.flashsale.config.RabbitMQConfig;
import com.example.flashsale.dto.NotificationMessage;
import com.example.flashsale.dto.OrderMessage;
import com.example.flashsale.entity.Order;
import com.example.flashsale.entity.OrderStatus;
import com.example.flashsale.entity.Product;
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
    private final SseService sseService;

    public Long decrementStock(Long productId, Integer quantity) {
        String stockKey = "product:stock:" + productId;
        return redisTemplate.execute(
                stockDecrementScript,
                Collections.singletonList(stockKey),
                String.valueOf(quantity)
        );
    }

    public boolean sendOrderToQueue(OrderMessage message) {
        try {
            rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY, message);
            return true;
        } catch (Exception e) {
            log.error("Lỗi gửi RabbitMQ: {}", e.getMessage());
            return false;
        }
    }

    public boolean processOrderAsync(OrderMessage message) {
        if (message.quantity() == null || message.quantity() <= 0) {
            redisTemplate.delete("idempotency:" + message.idempotencyKey());
            return false;
        }

        String stockKey = "product:stock:" + message.productId();

        Long result = redisTemplate.execute(
                stockDecrementScript,
                Collections.singletonList(stockKey),
                String.valueOf(message.quantity())
        );

        if (result == null || result == 0) {
            // Hết hàng -> Xóa key idempotency để client có thể retry nếu có đợt hoàn hàng
            redisTemplate.delete("idempotency:" + message.idempotencyKey());
            return false;
        }
        if (result == -1) {
            redisTemplate.delete("idempotency:" + message.idempotencyKey());
            throw new IllegalStateException("Sản phẩm chưa được warm cache!");
        }
        if (result == -2) {
            redisTemplate.delete("idempotency:" + message.idempotencyKey());
            throw new IllegalArgumentException("Số lượng sản phẩm mua không hợp lệ!");
        }

        try {
            // Gửi duy nhất 1 message vào pipeline xử lý đơn hàng
            rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.ROUTING_KEY, message);

            redisTemplate.opsForValue().set("order:status:" + message.idempotencyKey(), "PENDING", Duration.ofMinutes(10));
            redisTemplate.opsForValue().set("idempotency:" + message.idempotencyKey(), "ACCEPTED", Duration.ofMinutes(10));

            // Bắn SSE cập nhật tồn kho tức thì đến toàn bộ người dùng
            try {
                String newStockStr = redisTemplate.opsForValue().get(stockKey);
                if (newStockStr != null && sseService != null) {
                    sseService.broadcastStock(message.productId(), Integer.parseInt(newStockStr));
                }
            } catch (Exception ignored) {}

            return true;
        } catch (Exception e) {
            log.error("Lỗi gửi RabbitMQ, hoàn kho Redis: {}", e.getMessage());
            redisTemplate.opsForValue().increment(stockKey, message.quantity());
            redisTemplate.delete("idempotency:" + message.idempotencyKey());
            redisTemplate.delete("order:status:" + message.idempotencyKey());
            return false;
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public void saveOrderAndDecrementDbStock(OrderMessage message) {
        int updatedRows = productRepository.decrementStock(message.productId(), message.quantity());
        if (updatedRows == 0) {
            throw new IllegalStateException("Kho PostgreSQL đã hết!");
        }

        Product product = productRepository.findById(message.productId()).orElse(null);
        String pName = product != null ? product.getName() : "Thiết bị công nghệ TechShop";
        String pImg = product != null ? product.getImageUrl() : "https://images.unsplash.com/photo-1695048133142-1a20484d2569?auto=format&fit=crop&w=400&q=80";

        Long unitPrice = message.unitPrice();
        if (unitPrice == null) {
            unitPrice = (product != null && product.getPrice() != null) ? product.getPrice() : 0L;
        }
        if (unitPrice == null) {
            unitPrice = 0L;
        }
        Long discount = message.discountAmount() != null ? message.discountAmount() : 0L;
        Long finalAmount = message.finalAmount();
        if (finalAmount == null) {
            finalAmount = Math.max(0L, (unitPrice * message.quantity()) - discount);
        }

        String tracking = "TS" + (System.currentTimeMillis() % 10000000L);
        String invoice = "HD-2026-" + (10000 + (System.currentTimeMillis() % 90000));

        Order order = Order.builder()
                .idempotencyKey(message.idempotencyKey())
                .userId(message.userId())
                .productId(message.productId())
                .productName(pName)
                .productImage(pImg)
                .quantity(message.quantity())
                .unitPrice(unitPrice)
                .discountAmount(discount)
                .finalAmount(finalAmount)
                .voucherCode(message.voucherCode())
                .receiverName(message.receiverName())
                .receiverPhone(message.receiverPhone())
                .shippingAddress(message.shippingAddress())
                .itemsJson(message.itemsJson())
                .trackingCode(tracking)
                .shippingCarrier("TechShop Express (Hỏa Tốc 2H)")
                .shippingStatus("CONFIRMED")
                .invoiceNo(invoice)
                .status(OrderStatus.PENDING) // Giữ hàng chờ thanh toán
                .createdAt(LocalDateTime.now())
                .build();

        orderRepository.saveAndFlush(order);
    }

    // Cập nhật trạng thái vận chuyển từ Admin (CONFIRMED -> PACKING -> SHIPPING -> DELIVERED)
    @Transactional(rollbackFor = Exception.class)
    public boolean updateShippingStatus(String idempotencyKey, String newShippingStatus) {
        return orderRepository.findByIdempotencyKey(idempotencyKey).map(order -> {
            order.setShippingStatus(newShippingStatus);
            orderRepository.save(order);

            // Gửi thông báo đến người dùng qua RabbitMQ
            try {
                String statusTitle = switch (newShippingStatus) {
                    case "PACKING" -> "📦 Đơn hàng #" + order.getIdempotencyKey() + " đang được đóng gói tại kho";
                    case "SHIPPING" -> "🚚 Đơn hàng #" + order.getIdempotencyKey() + " đang được giao hỏa tốc";
                    case "DELIVERED" -> "✅ Đơn hàng #" + order.getIdempotencyKey() + " đã giao thành công";
                    default -> "ℹ️ Cập nhật vận đơn #" + order.getIdempotencyKey();
                };
                NotificationMessage noti = new NotificationMessage(
                        order.getIdempotencyKey(),
                        order.getUserId(),
                        order.getReceiverName() != null ? order.getReceiverName() : "Khách hàng",
                        order.getProductName() != null ? order.getProductName() : "Thiết bị công nghệ",
                        order.getFinalAmount() != null ? order.getFinalAmount() : 0L,
                        "SHIPPING_UPDATE",
                        statusTitle,
                        "Vận chuyển TechShop Express thông báo: Đơn hàng " + order.getIdempotencyKey() + " đã chuyển sang trạng thái: [" + newShippingStatus + "].",
                        LocalDateTime.now()
                );
                rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.NOTIFICATION_ROUTING_KEY, noti);
            } catch (Exception ignored) {}

            return true;
        }).orElse(false);
    }

    // Xác nhận thanh toán thành công (Atomic status transition PENDING -> SUCCESS)
    @Transactional(rollbackFor = Exception.class)
    public boolean confirmPayment(String idempotencyKey) {
        int updated = orderRepository.updateStatusIfCurrentStatus(
                idempotencyKey, OrderStatus.SUCCESS, OrderStatus.PENDING
        );
        if (updated == 0) {
            return false;
        }
        redisTemplate.opsForValue().set("order:status:" + idempotencyKey, "SUCCESS", Duration.ofMinutes(10));

        // Bắn sự kiện Notification sinh hóa đơn & gửi thông báo bất đồng bộ qua RabbitMQ
        try {
            orderRepository.findByIdempotencyKey(idempotencyKey).ifPresent(order -> {
                String pName = productRepository.findById(order.getProductId())
                        .map(Product::getName).orElse("Thiết bị công nghệ");
                long amt = order.getFinalAmount() != null ? order.getFinalAmount()
                        : (order.getUnitPrice() != null ? order.getUnitPrice() * order.getQuantity() : 0L);

                NotificationMessage notiMsg = new NotificationMessage(
                        order.getIdempotencyKey(),
                        order.getUserId(),
                        order.getReceiverName() != null ? order.getReceiverName() : "Khách hàng",
                        pName,
                        amt,
                        "ORDER_SUCCESS",
                        "🎉 Đặt hàng thành công #" + order.getIdempotencyKey(),
                        "TechShop đã nhận được thanh toán qua VietQR cho " + pName + " (SL: " + order.getQuantity() + "). Đơn hàng đang được đóng gói và xuất kho giao hỏa tốc 2H!",
                        LocalDateTime.now()
                );
                rabbitTemplate.convertAndSend(RabbitMQConfig.EXCHANGE_NAME, RabbitMQConfig.NOTIFICATION_ROUTING_KEY, notiMsg);
            });
        } catch (Exception ex) {
            log.warn("Lỗi gửi notification event: {}", ex.getMessage());
        }

        return true;
    }

    // Tự động thu hồi máy nếu quá hạn 30s chưa thanh toán (Atomic status transition PENDING -> EXPIRED)
    @Transactional(rollbackFor = Exception.class)
    public void expireUnpaidOrder(OrderMessage message) {
        int updated = orderRepository.updateStatusIfCurrentStatus(
                message.idempotencyKey(), OrderStatus.EXPIRED, OrderStatus.PENDING
        );

        if (updated > 0) {
            log.warn("Đơn hàng [{}] quá hạn thanh toán! Đang thu hồi {} sản phẩm về kho...",
                    message.idempotencyKey(), message.quantity());

            // Hoàn kho DB & Redis
            productRepository.incrementStock(message.productId(), message.quantity());
            redisTemplate.opsForValue().increment("product:stock:" + message.productId(), message.quantity());
            redisTemplate.opsForValue().set("order:status:" + message.idempotencyKey(), "EXPIRED", Duration.ofMinutes(10));

            // Bắn SSE cập nhật tồn kho phục hồi đến toàn bộ người dùng
            try {
                String restoredStockStr = redisTemplate.opsForValue().get("product:stock:" + message.productId());
                if (restoredStockStr != null && sseService != null) {
                    sseService.broadcastStock(message.productId(), Integer.parseInt(restoredStockStr));
                }
            } catch (Exception ignored) {}
        } else {
            log.info("Bỏ qua thu hồi đơn [{}]: Đơn đã thanh toán SUCCESS hoặc đã được xử lý trước đó.",
                    message.idempotencyKey());
        }
    }
}
