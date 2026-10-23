package com.example.flashsale.dto;

import java.io.Serializable;

public record OrderMessage(
        String idempotencyKey,
        Long userId,
        Long productId,
        Integer quantity
) implements Serializable {}