package com.example.flashsale.initializer;

import com.example.flashsale.entity.Product;
import com.example.flashsale.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class DataInitializer implements CommandLineRunner {

    private final ProductRepository productRepository;
    private final StringRedisTemplate redisTemplate;

    @Override
    public void run(String... args) {
        List<Product> products = productRepository.findAll();
        Product product;

        if (products.isEmpty()) {
            log.info("Chưa có sản phẩm trong DB, khởi tạo sản phẩm mẫu...");
            // Không gán thủ công id và version để Hibernate tự quản lý
            product = productRepository.save(
                    Product.builder()
                            .name("Vé Concert Flash Sale")
                            .stock(50)
                            .build()
            );
        } else {
            product = products.get(0);
        }

        // Nạp tồn kho sẵn lên Redis
        String redisStockKey = "product:stock:" + product.getId();
        redisTemplate.opsForValue().set(redisStockKey, String.valueOf(product.getStock()));

        log.info(">> Đã Warm Cache thành công: Product ID = {}, Key [{}] = {}",
                product.getId(), redisStockKey, product.getStock());
    }
}