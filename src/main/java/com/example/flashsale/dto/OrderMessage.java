package com.example.flashsale.dto;

import java.io.Serializable;

public record OrderMessage(
        String idempotencyKey,
        Long userId,
        Long productId,
        Integer quantity,
        Long unitPrice,
        Long discountAmount,
        Long finalAmount,
        String voucherCode,
        String receiverName,
        String receiverPhone,
        String shippingAddress,
        String itemsJson
) implements Serializable {

    // Constructor tương thích ngược cho unit test và load test cũ
    public OrderMessage(String idempotencyKey, Long userId, Long productId, Integer quantity) {
        this(idempotencyKey, userId, productId, quantity, null, 0L, null, null, null, null, null, null);
    }

    public OrderMessage(String idempotencyKey, Long userId, Long productId, Integer quantity,
                        Long unitPrice, Long discountAmount, Long finalAmount,
                        String voucherCode, String receiverName, String receiverPhone, String shippingAddress) {
        this(idempotencyKey, userId, productId, quantity, unitPrice, discountAmount, finalAmount,
                voucherCode, receiverName, receiverPhone, shippingAddress, null);
    }
}
