package com.example.flashsale.repository;

import com.example.flashsale.entity.Order;
import com.example.flashsale.entity.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface OrderRepository extends JpaRepository<Order, Long> {

    Optional<Order> findByIdempotencyKey(String idempotencyKey);

    List<Order> findByUserIdOrderByCreatedAtDesc(Long userId);

    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE Order o SET o.status = :newStatus WHERE o.idempotencyKey = :idempotencyKey AND o.status = :expectedStatus")
    int updateStatusIfCurrentStatus(
            @Param("idempotencyKey") String idempotencyKey,
            @Param("newStatus") OrderStatus newStatus,
            @Param("expectedStatus") OrderStatus expectedStatus
    );
}
