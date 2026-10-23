package com.example.flashsale.controller;

import com.example.flashsale.dto.OrderMessage;
import com.example.flashsale.entity.Product;
import com.example.flashsale.repository.OrderRepository;
import com.example.flashsale.repository.ProductRepository;
import com.example.flashsale.service.FlashSaleService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/flash-sale")
@RequiredArgsConstructor
@CrossOrigin("*")
public class FlashSaleController {

    private final FlashSaleService flashSaleService;
    private final StringRedisTemplate redisTemplate;
    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;

    // 1. API Đặt mua vé
    @PostMapping("/purchase")
    public ResponseEntity<?> purchase(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestParam Long userId,
            @RequestParam Long productId,
            @RequestParam Integer quantity
    ) {
        OrderMessage message = new OrderMessage(idempotencyKey, userId, productId, quantity);
        boolean accepted = flashSaleService.processOrderAsync(message);

        if (!accepted) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("message", "Sản phẩm đã hết hàng hoặc không khả dụng."));
        }

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of(
                        "status", "PENDING",
                        "idempotencyKey", idempotencyKey,
                        "checkStatusUrl", "/api/v1/flash-sale/status/" + idempotencyKey
                ));
    }

    // 2. API Kiểm tra trạng thái đơn (Polling)
    @GetMapping("/status/{idempotencyKey}")
    public ResponseEntity<?> checkStatus(@PathVariable String idempotencyKey) {
        String status = redisTemplate.opsForValue().get("order:status:" + idempotencyKey);
        if (status == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("message", "Không tìm thấy giao dịch."));
        }
        return ResponseEntity.ok(Map.of("idempotencyKey", idempotencyKey, "status", status));
    }

    // 3. API Xác nhận thanh toán trong vòng 30 giây
    @PostMapping("/pay/{idempotencyKey}")
    public ResponseEntity<?> payOrder(@PathVariable String idempotencyKey) {
        boolean paid = flashSaleService.confirmPayment(idempotencyKey);
        if (!paid) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("message", "Đơn hàng không tồn tại, đã thanh toán hoặc đã quá hạn giữ vé 30s!"));
        }
        return ResponseEntity.ok(Map.of("idempotencyKey", idempotencyKey, "status", "SUCCESS"));
    }

    // 4. API Lấy thông tin tồn kho Realtime (Redis vs PostgreSQL) cho Frontend
    @GetMapping("/product/{productId}")
    public ResponseEntity<?> getProductStock(@PathVariable Long productId) {
        Product product = productRepository.findById(productId).orElse(null);
        if (product == null) {
            return ResponseEntity.notFound().build();
        }
        String redisStockStr = redisTemplate.opsForValue().get("product:stock:" + productId);
        int redisStock = redisStockStr != null ? Integer.parseInt(redisStockStr) : 0;
        long totalOrders = orderRepository.count();

        return ResponseEntity.ok(Map.of(
                "id", product.getId(),
                "name", product.getName(),
                "dbStock", product.getStock(),
                "redisStock", redisStock,
                "totalOrders", totalOrders,
                "version", product.getVersion()
        ));
    }

    // 5. API Reset nhanh tồn kho về 50 vé để tiện Demo nhiều lần
    @PostMapping("/reset/{productId}")
    public ResponseEntity<?> resetStock(@PathVariable Long productId) {
        orderRepository.deleteAll();
        Product product = productRepository.findById(productId).orElseThrow();
        product.setStock(50);
        productRepository.save(product);
        redisTemplate.opsForValue().set("product:stock:" + productId, "50");

        return ResponseEntity.ok(Map.of("message", "Đã reset kho Redis & PostgreSQL về 50 vé!"));
    }
    @GetMapping("/orders")
    public ResponseEntity<?> getRecentOrders() {
        return ResponseEntity.ok(
                orderRepository.findAll(org.springframework.data.domain.Sort.by(
                        org.springframework.data.domain.Sort.Direction.DESC, "createdAt"
                )).stream().limit(20).toList()
        );
    }
}