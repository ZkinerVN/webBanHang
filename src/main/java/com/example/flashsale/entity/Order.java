package com.example.flashsale.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "orders", indexes = {
        @Index(name = "idx_order_idempotency", columnList = "idempotencyKey", unique = true),
        @Index(name = "idx_order_user_id", columnList = "userId"),
        @Index(name = "idx_order_product_status", columnList = "productId, status"),
        @Index(name = "idx_order_created_at", columnList = "createdAt")
})
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, unique = true)
    private String idempotencyKey;

    @Column(nullable = false)
    private Long userId;

    @Column(nullable = false)
    private Long productId;

    @Column(nullable = false)
    private Integer quantity;

    private Long unitPrice;

    private Long discountAmount;

    private Long finalAmount;

    private String voucherCode;

    private String receiverName;

    private String receiverPhone;

    private String shippingAddress;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    @Column(columnDefinition = "TEXT")
    private String itemsJson;

    private String productName;

    private String productImage;

    private String trackingCode;

    private String shippingCarrier;

    private String shippingStatus;

    private String invoiceNo;

    @Column(nullable = false)
    private LocalDateTime createdAt;
}
