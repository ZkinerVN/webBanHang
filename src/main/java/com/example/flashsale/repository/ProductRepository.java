package com.example.flashsale.repository;

import com.example.flashsale.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {

    @Modifying
    @Query("UPDATE Product p SET p.stock = p.stock - :quantity, p.version = p.version + 1 WHERE p.id = :id AND p.stock >= :quantity")
    int decrementStock(@Param("id") Long id, @Param("quantity") Integer quantity);

    @Modifying
    @Query("UPDATE Product p SET p.stock = p.stock + :quantity, p.version = p.version + 1 WHERE p.id = :id")
    int incrementStock(@Param("id") Long id, @Param("quantity") Integer quantity);
}