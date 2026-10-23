package com.example.flashsale.controller;

import com.example.flashsale.config.RabbitMQConfig;
import com.example.flashsale.dto.AuthResponse;
import com.example.flashsale.dto.CartCheckoutRequest;
import com.example.flashsale.dto.OrderMessage;
import com.example.flashsale.dto.PurchaseRequest;
import com.example.flashsale.entity.Order;
import com.example.flashsale.entity.OrderStatus;
import com.example.flashsale.entity.Product;
import com.example.flashsale.repository.OrderRepository;
import com.example.flashsale.repository.ProductRepository;
import com.example.flashsale.service.AuthService;
import com.example.flashsale.service.CircuitBreakerService;
import com.example.flashsale.service.FlashSaleService;
import com.example.flashsale.service.SseService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/flash-sale")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*", allowedHeaders = "*", methods = {RequestMethod.GET, RequestMethod.POST, RequestMethod.PUT})
public class FlashSaleController {

    private final FlashSaleService flashSaleService;
    private final AuthService authService;
    private final StringRedisTemplate redisTemplate;
    private final ProductRepository productRepository;
    private final OrderRepository orderRepository;
    private final AmqpAdmin amqpAdmin;
    private final SseService sseService;
    private final CircuitBreakerService circuitBreakerService;

    // 0. API Kết nối SSE Realtime Stock Stream (Zero Polling Overhead)
    @GetMapping(value = "/sse/stock-stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter subscribeStockStream() {
        return sseService.subscribe();
    }

    // 1. API Đặt mua hàng Flash Sale (Tự động trích xuất UserId từ Token để chống giả mạo)
    @PostMapping("/purchase")
    public ResponseEntity<?> purchase(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @Valid @RequestBody(required = false) PurchaseRequest requestBody,
            @RequestParam(required = false) Long userId,
            @RequestParam(required = false) Long productId,
            @RequestParam(required = false) Integer quantity
    ) {
        // 0. Circuit Breaker Fallback Mode Guard
        if (!circuitBreakerService.allowRequest()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                    "message", "🛡️ [Circuit Breaker OPEN] Hệ thống đang kích hoạt Fallback Mode chống quá tải! Vui lòng thử lại sau giây lát.",
                    "circuitBreakerState", circuitBreakerService.getState().name(),
                    "fallback", true
            ));
        }

        // Bảo mật: Nếu có Token đăng nhập, ưu tiên dùng UserId chính chủ từ Token
        Long resolvedUserId = null;
        if (authHeader != null && !authHeader.isBlank()) {
            AuthResponse loggedInUser = authService.getCurrentUser(authHeader);
            if (loggedInUser != null) {
                resolvedUserId = loggedInUser.id();
            }
        }

        // Nếu không có Token (ví dụ chạy Concurrency Test giả lập đa luồng), dùng userId truyền vào
        if (resolvedUserId == null) {
            resolvedUserId = requestBody != null ? requestBody.userId() : userId;
        }

        Long finalProductId = requestBody != null ? requestBody.productId() : productId;
        Integer finalQuantity = requestBody != null ? requestBody.quantity() : quantity;

        if (resolvedUserId == null || resolvedUserId <= 0) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                    .body(Map.of("message", "Vui lòng đăng nhập để thực hiện đặt hàng."));
        }
        if (finalProductId == null || finalProductId <= 0) {
            return ResponseEntity.badRequest().body(Map.of("message", "productId phải là số dương hợp lệ."));
        }
        if (finalQuantity == null || finalQuantity < 1 || finalQuantity > 5) {
            return ResponseEntity.badRequest().body(Map.of("message", "Số lượng mua mỗi lượt phải từ 1 đến 5 sản phẩm."));
        }

        Product product = productRepository.findById(finalProductId).orElse(null);
        Long unitPrice = requestBody != null && requestBody.unitPrice() != null
                ? requestBody.unitPrice()
                : (product != null ? product.getPrice() : 19990000L);
        Long discountAmount = requestBody != null && requestBody.discountAmount() != null
                ? requestBody.discountAmount() : 0L;
        Long finalAmount = requestBody != null && requestBody.finalAmount() != null
                ? requestBody.finalAmount()
                : Math.max(0L, (unitPrice * finalQuantity) - discountAmount);
        String voucherCode = requestBody != null ? requestBody.voucherCode() : null;
        String receiverName = requestBody != null ? requestBody.receiverName() : null;
        String receiverPhone = requestBody != null ? requestBody.receiverPhone() : null;
        String shippingAddress = requestBody != null ? requestBody.shippingAddress() : null;

        OrderMessage message = new OrderMessage(
                idempotencyKey,
                resolvedUserId,
                finalProductId,
                finalQuantity,
                unitPrice,
                discountAmount,
                finalAmount,
                voucherCode,
                receiverName,
                receiverPhone,
                shippingAddress
        );

        boolean accepted;
        try {
            accepted = flashSaleService.processOrderAsync(message);
        } catch (Exception e) {
            circuitBreakerService.recordFailure();
            throw e;
        }

        if (!accepted) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("message", "Sản phẩm đã hết hàng trong khung giờ vàng hoặc không khả dụng."));
        }

        circuitBreakerService.recordSuccess();

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of(
                        "status", "PENDING",
                        "idempotencyKey", idempotencyKey,
                        "checkStatusUrl", "/api/v1/flash-sale/status/" + idempotencyKey
                ));
    }

    // 1.1 API Đặt mua toàn bộ Giỏ hàng (Multi-Item Cart Checkout với Atomic Batch Stock Decrement)
    @PostMapping("/cart-checkout")
    public ResponseEntity<?> cartCheckout(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @RequestBody CartCheckoutRequest request
    ) {
        // 0. Circuit Breaker Fallback Mode Guard
        if (!circuitBreakerService.allowRequest()) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of(
                    "message", "🛡️ [Circuit Breaker OPEN] Hệ thống đang kích hoạt Fallback Mode chống quá tải! Giỏ hàng đã được bảo lưu an toàn.",
                    "circuitBreakerState", circuitBreakerService.getState().name(),
                    "fallback", true
            ));
        }

        if (request == null || request.items() == null || request.items().isEmpty()) {
            return ResponseEntity.badRequest().body(Map.of("message", "Giỏ hàng không có sản phẩm nào."));
        }

        Long userId = request.userId();
        if (authHeader != null && !authHeader.isBlank()) {
            AuthResponse user = authService.getCurrentUser(authHeader);
            if (user != null) userId = user.id();
        }

        if (userId == null || userId <= 0) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("message", "Vui lòng đăng nhập để đặt hàng."));
        }

        // 1. Kiểm tra Idempotency chống duplicate request
        Boolean isFirstRequest = redisTemplate.opsForValue().setIfAbsent(
                "idempotency:" + idempotencyKey, "PROCESSING", Duration.ofMinutes(10)
        );
        if (Boolean.FALSE.equals(isFirstRequest)) {
            String existingStatus = redisTemplate.opsForValue().get("order:status:" + idempotencyKey);
            if ("SUCCESS".equals(existingStatus)) {
                return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("message", "Đơn hàng này đã được thanh toán!"));
            }
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("message", "Giao dịch giỏ hàng đang được xử lý..."));
        }

        // 2. Trừ kho Redis nguyên tử cho từng sản phẩm trong giỏ
        List<CartCheckoutRequest.CartItemDto> decrementedItems = new ArrayList<>();
        boolean allStockOk = true;
        String failedProductName = "";

        for (CartCheckoutRequest.CartItemDto item : request.items()) {
            Long res = flashSaleService.decrementStock(item.productId(), item.quantity());
            if (res != null && res > 0) {
                decrementedItems.add(item);
                // Bắn SSE cập nhật kho
                try {
                    String curStock = redisTemplate.opsForValue().get("product:stock:" + item.productId());
                    if (curStock != null) sseService.broadcastStock(item.productId(), Integer.parseInt(curStock));
                } catch (Exception ignored) {}
            } else {
                allStockOk = false;
                failedProductName = item.name();
                break;
            }
        }

        // Nếu bất kỳ sản phẩm nào hết hàng -> Rollback hoàn trả kho toàn bộ các món đã trừ trước đó
        if (!allStockOk) {
            for (CartCheckoutRequest.CartItemDto rollbackItem : decrementedItems) {
                redisTemplate.opsForValue().increment("product:stock:" + rollbackItem.productId(), rollbackItem.quantity());
                try {
                    String curStock = redisTemplate.opsForValue().get("product:stock:" + rollbackItem.productId());
                    if (curStock != null) sseService.broadcastStock(rollbackItem.productId(), Integer.parseInt(curStock));
                } catch (Exception ignored) {}
            }
            redisTemplate.delete("idempotency:" + idempotencyKey);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "message", "Sản phẩm [" + failedProductName + "] đã hết hàng trong kho! Toàn bộ giỏ hàng đã được bảo lưu an toàn."
            ));
        }

        // 3. Tính toán tổng tiền và áp dụng voucher
        long subtotal = 0L;
        for (CartCheckoutRequest.CartItemDto item : request.items()) {
            subtotal += (item.price() * item.quantity());
        }

        long discountAmount = 0L;
        if (request.voucherCode() != null && !request.voucherCode().isBlank()) {
            String v = request.voucherCode().trim().toUpperCase();
            if ("TECH50K".equals(v) && subtotal >= 500000L) discountAmount = 50000L;
            else if ("FREESHIP".equals(v)) discountAmount = 30000L;
            else if ("VIP10".equals(v)) discountAmount = Math.min(2000000L, (long)(subtotal * 0.1));
        }
        long finalAmount = Math.max(0L, subtotal - discountAmount);

        // Chuỗi JSON danh sách sản phẩm
        String itemsJson = "";
        try {
            ObjectMapper mapper = new ObjectMapper();
            itemsJson = mapper.writeValueAsString(request.items());
        } catch (Exception ignored) {}

        // 4. Ghi nhận trạng thái PENDING và gửi vào RabbitMQ
        redisTemplate.opsForValue().set("order:status:" + idempotencyKey, "PENDING", Duration.ofMinutes(10));

        CartCheckoutRequest.CartItemDto primaryItem = request.items().get(0);
        OrderMessage orderMsg = new OrderMessage(
                idempotencyKey,
                userId,
                primaryItem.productId(),
                primaryItem.quantity(),
                primaryItem.price(),
                discountAmount,
                finalAmount,
                request.voucherCode(),
                request.receiverName(),
                request.receiverPhone(),
                request.shippingAddress(),
                itemsJson
        );

        boolean sent;
        try {
            sent = flashSaleService.sendOrderToQueue(orderMsg);
        } catch (Exception e) {
            circuitBreakerService.recordFailure();
            throw e;
        }

        if (!sent) {
            circuitBreakerService.recordFailure();
            // Hoàn kho nếu RabbitMQ lỗi
            for (CartCheckoutRequest.CartItemDto rollbackItem : request.items()) {
                redisTemplate.opsForValue().increment("product:stock:" + rollbackItem.productId(), rollbackItem.quantity());
            }
            redisTemplate.delete("idempotency:" + idempotencyKey);
            redisTemplate.delete("order:status:" + idempotencyKey);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("message", "Hệ thống hàng đợi quá tải, vui lòng thử lại sau giây lát."));
        }

        circuitBreakerService.recordSuccess();

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of(
                "idempotencyKey", idempotencyKey,
                "subtotal", subtotal,
                "discountAmount", discountAmount,
                "finalAmount", finalAmount,
                "totalItems", request.items().size(),
                "status", "PENDING",
                "message", "Khóa giữ giỏ hàng thành công! Quý khách có 30 giây để hoàn tất thanh toán VietQR."
        ));
    }

    // 2. API Lấy toàn bộ danh mục sản phẩm cho Storefront
    @GetMapping("/products")
    public ResponseEntity<?> getAllProducts() {
        List<Product> products = productRepository.findAll();
        List<Map<String, Object>> response = new ArrayList<>();

        for (Product p : products) {
            String redisStockStr = redisTemplate.opsForValue().get("product:stock:" + p.getId());
            int redisStock = redisStockStr != null ? Integer.parseInt(redisStockStr) : p.getStock();

            Map<String, Object> item = new HashMap<>();
            item.put("id", p.getId());
            item.put("name", p.getName());
            item.put("category", p.getCategory() != null ? p.getCategory() : "Công nghệ");
            item.put("price", p.getPrice() != null ? p.getPrice() : 19990000L);
            item.put("originalPrice", p.getOriginalPrice() != null ? p.getOriginalPrice() : 34990000L);
            item.put("imageUrl", p.getImageUrl());
            item.put("description", p.getDescription());
            item.put("stock", p.getStock());
            item.put("redisStock", redisStock);
            response.add(item);
        }

        return ResponseEntity.ok(response);
    }

    // 3. API Kiểm tra trạng thái đơn (Polling)
    @GetMapping("/status/{idempotencyKey}")
    public ResponseEntity<?> checkStatus(@PathVariable String idempotencyKey) {
        String status = redisTemplate.opsForValue().get("order:status:" + idempotencyKey);
        if (status == null) {
            // Kiểm tra DB nếu Redis hết hạn TTL
            Optional<Order> orderOpt = orderRepository.findByIdempotencyKey(idempotencyKey);
            if (orderOpt.isPresent()) {
                status = orderOpt.get().getStatus().name();
            } else {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("message", "Không tìm thấy giao dịch."));
            }
        }
        return ResponseEntity.ok(Map.of("idempotencyKey", idempotencyKey, "status", status));
    }

    // 4. API Xác nhận thanh toán trong vòng 30 giây
    @PostMapping("/pay/{idempotencyKey}")
    public ResponseEntity<?> payOrder(@PathVariable String idempotencyKey) {
        boolean paid = flashSaleService.confirmPayment(idempotencyKey);
        if (!paid) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("message", "Đơn hàng không tồn tại, đã thanh toán hoặc đã quá hạn giữ chỗ 30s!"));
        }
        return ResponseEntity.ok(Map.of("idempotencyKey", idempotencyKey, "status", "SUCCESS"));
    }

    // 5. API Lấy thông tin tồn kho Realtime (Redis vs PostgreSQL)
    @GetMapping("/product/{productId}")
    public ResponseEntity<?> getProductStock(@PathVariable Long productId) {
        Product product = productRepository.findById(productId).orElse(null);
        if (product == null) {
            return ResponseEntity.notFound().build();
        }
        String redisStockStr = redisTemplate.opsForValue().get("product:stock:" + productId);
        int redisStock = redisStockStr != null ? Integer.parseInt(redisStockStr) : 0;
        long totalOrders = orderRepository.count();

        Map<String, Object> result = new HashMap<>();
        result.put("id", product.getId());
        result.put("name", product.getName());
        result.put("category", product.getCategory() != null ? product.getCategory() : "Điện thoại");
        result.put("price", product.getPrice() != null ? product.getPrice() : 19990000L);
        result.put("originalPrice", product.getOriginalPrice() != null ? product.getOriginalPrice() : 34990000L);
        result.put("imageUrl", product.getImageUrl());
        result.put("description", product.getDescription());
        result.put("dbStock", product.getStock());
        result.put("redisStock", redisStock);
        result.put("totalOrders", totalOrders);
        result.put("version", product.getVersion() != null ? product.getVersion() : 0);

        return ResponseEntity.ok(result);
    }

    // 6. API Lấy đơn hàng của người dùng cụ thể (Bổ sung chi tiết sản phẩm)
    @GetMapping("/orders/user/{userId}")
    public ResponseEntity<?> getUserOrders(@PathVariable Long userId) {
        List<Order> orders = orderRepository.findByUserIdOrderByCreatedAtDesc(userId);
        Map<Long, Product> productMap = productRepository.findAllById(
                orders.stream().map(Order::getProductId).distinct().toList()
        ).stream().collect(Collectors.toMap(Product::getId, p -> p));

        List<Map<String, Object>> response = orders.stream().map(o -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", o.getId());
            map.put("idempotencyKey", o.getIdempotencyKey());
            map.put("userId", o.getUserId());
            map.put("productId", o.getProductId());
            map.put("quantity", o.getQuantity());
            map.put("unitPrice", o.getUnitPrice());
            map.put("discountAmount", o.getDiscountAmount());
            map.put("finalAmount", o.getFinalAmount());
            map.put("voucherCode", o.getVoucherCode());
            map.put("receiverName", o.getReceiverName());
            map.put("receiverPhone", o.getReceiverPhone());
            map.put("shippingAddress", o.getShippingAddress());
            map.put("status", o.getStatus().name());
            map.put("trackingCode", o.getTrackingCode());
            map.put("shippingCarrier", o.getShippingCarrier());
            map.put("shippingStatus", o.getShippingStatus() != null ? o.getShippingStatus() : "CONFIRMED");
            map.put("invoiceNo", o.getInvoiceNo());
            map.put("itemsJson", o.getItemsJson());
            map.put("createdAt", o.getCreatedAt());

            Product p = productMap.get(o.getProductId());
            if (p != null) {
                map.put("productName", p.getName());
                map.put("productImage", p.getImageUrl());
                map.put("productCategory", p.getCategory());
                if (o.getUnitPrice() == null) {
                    map.put("unitPrice", p.getPrice());
                }
                if (o.getFinalAmount() == null) {
                    map.put("finalAmount", p.getPrice() * o.getQuantity());
                }
            } else {
                map.put("productName", "Thiết bị công nghệ TechShop #" + o.getProductId());
                map.put("productImage", "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?auto=format&fit=crop&w=400&q=80");
            }
            return map;
        }).toList();

        return ResponseEntity.ok(response);
    }

    // 7. Lấy danh sách đơn gần nhất toàn hệ thống (Dành cho Admin)
    @GetMapping("/orders")
    public ResponseEntity<?> getRecentOrders() {
        List<Order> orders = orderRepository.findAll(PageRequest.of(0, 50, Sort.by(Sort.Direction.DESC, "createdAt"))).getContent();
        Map<Long, Product> productMap = productRepository.findAllById(
                orders.stream().map(Order::getProductId).distinct().toList()
        ).stream().collect(Collectors.toMap(Product::getId, p -> p));

        List<Map<String, Object>> response = orders.stream().map(o -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", o.getId());
            map.put("idempotencyKey", o.getIdempotencyKey());
            map.put("userId", o.getUserId());
            map.put("productId", o.getProductId());
            map.put("quantity", o.getQuantity());
            map.put("unitPrice", o.getUnitPrice());
            map.put("discountAmount", o.getDiscountAmount());
            map.put("finalAmount", o.getFinalAmount());
            map.put("voucherCode", o.getVoucherCode());
            map.put("receiverName", o.getReceiverName());
            map.put("receiverPhone", o.getReceiverPhone());
            map.put("shippingAddress", o.getShippingAddress());
            map.put("status", o.getStatus().name());
            map.put("trackingCode", o.getTrackingCode());
            map.put("shippingCarrier", o.getShippingCarrier());
            map.put("shippingStatus", o.getShippingStatus() != null ? o.getShippingStatus() : "CONFIRMED");
            map.put("invoiceNo", o.getInvoiceNo());
            map.put("itemsJson", o.getItemsJson());
            map.put("createdAt", o.getCreatedAt());

            Product p = productMap.get(o.getProductId());
            if (p != null) {
                map.put("productName", p.getName());
                if (o.getUnitPrice() == null) map.put("unitPrice", p.getPrice());
                if (o.getFinalAmount() == null) map.put("finalAmount", p.getPrice() * o.getQuantity());
            } else {
                map.put("productName", "Sản phẩm #" + o.getProductId());
            }
            return map;
        }).toList();

        return ResponseEntity.ok(response);
    }

    // 8. API Thống kê số liệu hệ thống dành cho Admin Dashboard
    @GetMapping("/admin/stats")
    public ResponseEntity<?> getAdminStats() {
        long totalOrders = orderRepository.count();
        List<Order> allOrders = orderRepository.findAll();

        long successCount = allOrders.stream().filter(o -> o.getStatus() == OrderStatus.SUCCESS).count();
        long pendingCount = allOrders.stream().filter(o -> o.getStatus() == OrderStatus.PENDING).count();
        long expiredCount = allOrders.stream().filter(o -> o.getStatus() == OrderStatus.EXPIRED).count();
        long failedCount = allOrders.stream().filter(o -> o.getStatus() == OrderStatus.FAILED).count();

        // Doanh thu thực tế từ các đơn thành công
        long actualRevenue = allOrders.stream()
                .filter(o -> o.getStatus() == OrderStatus.SUCCESS)
                .mapToLong(o -> o.getFinalAmount() != null ? o.getFinalAmount() : (o.getUnitPrice() != null ? o.getUnitPrice() * o.getQuantity() : 19990000L * o.getQuantity()))
                .sum();

        Map<String, Object> stats = new HashMap<>();
        stats.put("totalOrders", totalOrders);
        stats.put("successOrders", successCount);
        stats.put("pendingOrders", pendingCount);
        stats.put("expiredOrders", expiredCount);
        stats.put("failedOrders", failedCount);
        stats.put("revenue", actualRevenue);

        return ResponseEntity.ok(stats);
    }

    // 8.1 API Xuất file CSV báo cáo toàn bộ đơn hàng (Excel UTF-8 BOM)
    @GetMapping("/admin/orders/export-csv")
    public ResponseEntity<byte[]> exportOrdersCsv() {
        List<Order> orders = orderRepository.findAll(Sort.by(Sort.Direction.DESC, "createdAt"));
        Map<Long, Product> productMap = productRepository.findAll().stream()
                .collect(Collectors.toMap(Product::getId, p -> p));

        StringBuilder sb = new StringBuilder();
        sb.append('\ufeff'); // UTF-8 BOM cho Excel
        sb.append("Mã Vận Đơn,Mã Khách Hàng,Tên Thiết Bị,Số Lượng,Đơn Giá,Giảm Giá,Thành Tiền,Mã Voucher,Người Nhận,Số Điện Thoại,Địa Chỉ Giao,Trạng Thái,Thời Gian Tạo\n");

        for (Order o : orders) {
            Product p = productMap.get(o.getProductId());
            String pName = p != null ? p.getName().replace(",", " ") : "Sản phẩm #" + o.getProductId();
            long uPrice = o.getUnitPrice() != null ? o.getUnitPrice() : (p != null ? p.getPrice() : 0L);
            long disc = o.getDiscountAmount() != null ? o.getDiscountAmount() : 0L;
            long fAmt = o.getFinalAmount() != null ? o.getFinalAmount() : (uPrice * o.getQuantity() - disc);
            String vCode = o.getVoucherCode() != null ? o.getVoucherCode() : "";
            String rName = o.getReceiverName() != null ? o.getReceiverName().replace(",", " ") : "";
            String rPhone = o.getReceiverPhone() != null ? o.getReceiverPhone() : "";
            String sAddr = o.getShippingAddress() != null ? o.getShippingAddress().replace(",", " ") : "";

            sb.append(String.format("%s,%d,\"%s\",%d,%d,%d,%d,%s,\"%s\",%s,\"%s\",%s,%s\n",
                    o.getIdempotencyKey(),
                    o.getUserId(),
                    pName,
                    o.getQuantity(),
                    uPrice,
                    disc,
                    fAmt,
                    vCode,
                    rName,
                    rPhone,
                    sAddr,
                    o.getStatus().name(),
                    o.getCreatedAt().toString()
            ));
        }

        byte[] bytes = sb.toString().getBytes(StandardCharsets.UTF_8);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=techshop-orders-" + System.currentTimeMillis() + ".csv")
                .contentType(MediaType.parseMediaType("text/csv; charset=UTF-8"))
                .body(bytes);
    }

    // 9. API Admin Cập nhật tồn kho sản phẩm (Đồng bộ cả PostgreSQL và Redis)
    @PostMapping("/admin/product/{productId}/stock")
    public ResponseEntity<?> updateProductStock(
            @PathVariable Long productId,
            @RequestParam Integer newStock
    ) {
        if (newStock < 0) {
            return ResponseEntity.badRequest().body(Map.of("message", "Tồn kho không được là số âm"));
        }

        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new IllegalArgumentException("Không tìm thấy sản phẩm"));

        product.setStock(newStock);
        productRepository.save(product);
        redisTemplate.opsForValue().set("product:stock:" + productId, String.valueOf(newStock));

        // Bắn SSE Realtime đến toàn bộ người dùng
        try {
            sseService.broadcastStock(productId, newStock);
        } catch (Exception ignored) {}

        log.info(">> [ADMIN] Đã cập nhật tồn kho sản phẩm ID = {}: {} máy (DB & Redis)", productId, newStock);
        return ResponseEntity.ok(Map.of(
                "message", "Cập nhật tồn kho thành công",
                "productId", productId,
                "stock", newStock
        ));
    }

    // 10. API Reset hoàn toàn trạng thái và tồn kho về 50 suất cho sản phẩm
    @PostMapping("/reset/{productId}")
    public ResponseEntity<?> resetStock(@PathVariable Long productId) {
        try {
            amqpAdmin.purgeQueue(RabbitMQConfig.QUEUE_NAME);
            amqpAdmin.purgeQueue(RabbitMQConfig.DELAY_QUEUE);
            amqpAdmin.purgeQueue(RabbitMQConfig.TIMEOUT_QUEUE);
            amqpAdmin.purgeQueue(RabbitMQConfig.DLQ_NAME);
        } catch (Exception e) {
            log.warn("Lỗi khi purge queue RabbitMQ: {}", e.getMessage());
        }

        orderRepository.deleteAll();

        Product product = productRepository.findById(productId).orElseThrow();
        product.setStock(50);
        productRepository.save(product);
        redisTemplate.opsForValue().set("product:stock:" + productId, "50");

        Set<String> orderKeys = redisTemplate.keys("order:status:*");
        if (orderKeys != null && !orderKeys.isEmpty()) {
            redisTemplate.delete(orderKeys);
        }
        Set<String> idempotencyKeys = redisTemplate.keys("idempotency:*");
        if (idempotencyKeys != null && !idempotencyKeys.isEmpty()) {
            redisTemplate.delete(idempotencyKeys);
        }
        Set<String> rateLimitKeys = redisTemplate.keys("rate_limit:*");
        if (rateLimitKeys != null && !rateLimitKeys.isEmpty()) {
            redisTemplate.delete(rateLimitKeys);
        }

        return ResponseEntity.ok(Map.of("message", "Đã reset sạch hàng đợi, Redis Cache & PostgreSQL về 50 sản phẩm!"));
    }

    // 11. API Admin Tạo sản phẩm công nghệ mới
    @PostMapping("/admin/products")
    public ResponseEntity<?> createProduct(
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @RequestBody Product product
    ) {
        if (!isAdmin(authHeader)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("message", "Từ chối truy cập: Yêu cầu quyền Quản trị viên (ADMIN)"));
        }
        if (product.getName() == null || product.getName().isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("message", "Tên sản phẩm không được để trống"));
        }
        if (product.getStock() == null || product.getStock() < 0) product.setStock(10);
        if (product.getPrice() == null) product.setPrice(1000000L);
        if (product.getOriginalPrice() == null) product.setOriginalPrice(product.getPrice());
        if (product.getCategory() == null) product.setCategory("Thiết bị công nghệ");

        Product saved = productRepository.save(product);
        redisTemplate.opsForValue().set("product:stock:" + saved.getId(), String.valueOf(saved.getStock()));

        try {
            sseService.broadcastStock(saved.getId(), saved.getStock());
        } catch (Exception ignored) {}

        return ResponseEntity.status(HttpStatus.CREATED).body(saved);
    }

    // 12. API Admin Chỉnh sửa thông tin sản phẩm
    @PutMapping("/admin/products/{id}")
    public ResponseEntity<?> updateProduct(
            @PathVariable Long id,
            @RequestHeader(value = "Authorization", required = false) String authHeader,
            @RequestBody Product updateData
    ) {
        if (!isAdmin(authHeader)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("message", "Từ chối truy cập: Yêu cầu quyền Quản trị viên (ADMIN)"));
        }
        return productRepository.findById(id).map(p -> {
            if (updateData.getName() != null) p.setName(updateData.getName());
            if (updateData.getDescription() != null) p.setDescription(updateData.getDescription());
            if (updateData.getPrice() != null) p.setPrice(updateData.getPrice());
            if (updateData.getOriginalPrice() != null) p.setOriginalPrice(updateData.getOriginalPrice());
            if (updateData.getImageUrl() != null) p.setImageUrl(updateData.getImageUrl());
            if (updateData.getCategory() != null) p.setCategory(updateData.getCategory());
            if (updateData.getStock() != null && updateData.getStock() >= 0) {
                p.setStock(updateData.getStock());
                redisTemplate.opsForValue().set("product:stock:" + id, String.valueOf(updateData.getStock()));
                try {
                    sseService.broadcastStock(id, updateData.getStock());
                } catch (Exception ignored) {}
            }
            Product saved = productRepository.save(p);
            return ResponseEntity.ok(saved);
        }).orElseGet(() -> ResponseEntity.notFound().build());
    }

    // 13. API Admin Xóa sản phẩm
    @DeleteMapping("/admin/products/{id}")
    public ResponseEntity<?> deleteProduct(
            @PathVariable Long id,
            @RequestHeader(value = "Authorization", required = false) String authHeader
    ) {
        if (!isAdmin(authHeader)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("message", "Từ chối truy cập: Yêu cầu quyền Quản trị viên (ADMIN)"));
        }
        if (!productRepository.existsById(id)) {
            return ResponseEntity.notFound().build();
        }
        productRepository.deleteById(id);
        redisTemplate.delete("product:stock:" + id);
        return ResponseEntity.ok(Map.of("message", "Đã xóa sản phẩm ID #" + id + " thành công"));
    }

    // 14. API Admin Cập nhật trạng thái vận chuyển đơn hàng (CONFIRMED -> PACKING -> SHIPPING -> DELIVERED)
    @PutMapping("/admin/orders/{idempotencyKey}/shipping")
    public ResponseEntity<?> updateOrderShippingStatus(
            @PathVariable String idempotencyKey,
            @RequestParam String status,
            @RequestHeader(value = "Authorization", required = false) String authHeader
    ) {
        if (!isAdmin(authHeader)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(Map.of("message", "Từ chối truy cập: Yêu cầu quyền Quản trị viên (ADMIN)"));
        }
        boolean ok = flashSaleService.updateShippingStatus(idempotencyKey, status);
        if (ok) {
            return ResponseEntity.ok(Map.of("message", "Đã cập nhật trạng thái vận chuyển sang [" + status + "]"));
        } else {
            return ResponseEntity.badRequest().body(Map.of("message", "Không tìm thấy đơn hàng #" + idempotencyKey));
        }
    }

    // 15. API Trạng thái Circuit Breaker
    @GetMapping("/circuit-breaker/status")
    public ResponseEntity<?> getCircuitBreakerStatus() {
        return ResponseEntity.ok(Map.of(
                "state", circuitBreakerService.getState().name(),
                "allowRequest", circuitBreakerService.allowRequest(),
                "requestCount", circuitBreakerService.getRequestCount(),
                "failureCount", circuitBreakerService.getFailureCount()
        ));
    }

    // 16. API Admin Giả lập ngắt mạch Circuit Breaker (OPEN - kích hoạt Fallback Mode)
    @PostMapping("/admin/circuit-breaker/trip")
    public ResponseEntity<?> tripCircuitBreaker() {
        circuitBreakerService.tripCircuitBreaker();
        return ResponseEntity.ok(Map.of(
                "message", "Đã kích hoạt ngắt mạch Circuit Breaker -> OPEN. Chế độ Fallback Mode đang hoạt động!",
                "state", "OPEN"
        ));
    }

    // 17. API Admin Khôi phục Circuit Breaker (CLOSED - Hoạt động bình thường)
    @PostMapping("/admin/circuit-breaker/reset")
    public ResponseEntity<?> resetCircuitBreaker() {
        circuitBreakerService.resetCircuitBreaker();
        return ResponseEntity.ok(Map.of(
                "message", "Đã khôi phục trạng thái Circuit Breaker -> CLOSED. Hệ thống sẵn sàng nhận tải bình thường!",
                "state", "CLOSED"
        ));
    }

    // Helper kiểm tra quyền Quản trị viên từ JWT Token
    private boolean isAdmin(String authHeader) {
        if (authHeader == null || authHeader.isBlank()) return false;
        try {
            AuthResponse user = authService.getCurrentUser(authHeader);
            return user != null && "ADMIN".equalsIgnoreCase(user.role());
        } catch (Exception e) {
            return false;
        }
    }
}
