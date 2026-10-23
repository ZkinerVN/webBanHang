package com.example.flashsale;

import com.example.flashsale.dto.OrderMessage;
import com.example.flashsale.entity.Product;
import com.example.flashsale.repository.OrderRepository;
import com.example.flashsale.repository.ProductRepository;
import com.example.flashsale.service.FlashSaleService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
class FlashSaleConcurrencyTest {

    @Autowired
    private FlashSaleService flashSaleService;

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private OrderRepository orderRepository;

    @Autowired
    private StringRedisTemplate redisTemplate;

    private Long testProductId;
    private static final int INITIAL_STOCK = 50;
    private static final int TOTAL_THREADS = 200;

    @BeforeEach
    void setUp() {
        orderRepository.deleteAll();
        productRepository.deleteAll();

        Product saved = productRepository.save(
                Product.builder()
                        .name("Vé Test Đa Luồng")
                        .stock(INITIAL_STOCK)
                        .build()
        );
        testProductId = saved.getId();

        // Warm cache
        redisTemplate.opsForValue().set("product:stock:" + testProductId, String.valueOf(INITIAL_STOCK));
    }

    @Test
    void shouldNotOversellWhen200UsersBuy50TicketsSimultaneously() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(TOTAL_THREADS);
        CountDownLatch readyLatch = new CountDownLatch(TOTAL_THREADS);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(TOTAL_THREADS);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        for (int i = 0; i < TOTAL_THREADS; i++) {
            final long userId = i + 1;
            executor.submit(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await(); // Chờ hiệu lệnh xuất phát cùng lúc
                    OrderMessage msg = new OrderMessage(UUID.randomUUID().toString(), userId, testProductId, 1);
                    if (flashSaleService.processOrderAsync(msg)) {
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();      // Đợi đủ 200 luồng vào vị trí
        startLatch.countDown();  // Bắn súng xuất phát đồng loạt!
        doneLatch.await();       // Đợi cả 200 luồng chạy xong
        executor.shutdown();

        // Đợi 3 giây cho Worker RabbitMQ tiêu thụ hết toàn bộ 50 message ghi xuống PostgreSQL
        Thread.sleep(3000);

        // KIỂM CHỨNG KẾT QUẢ
        assertEquals(50, successCount.get(), "Phải có đúng 50 lượt mua thành công");
        assertEquals(150, failCount.get(), "Phải có đúng 150 lượt bị chặn do hết hàng");

        String redisRemaining = redisTemplate.opsForValue().get("product:stock:" + testProductId);
        assertEquals("0", redisRemaining, "Tồn kho trên Redis phải bằng 0 (không được âm)");

        Product dbProduct = productRepository.findById(testProductId).orElseThrow();
        assertEquals(0, dbProduct.getStock(), "Tồn kho trong PostgreSQL phải bằng 0");

        assertEquals(50, orderRepository.count(), "Tổng số đơn hàng trong DB phải đúng bằng 50");
    }
}