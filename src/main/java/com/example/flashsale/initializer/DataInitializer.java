package com.example.flashsale.initializer;

import com.example.flashsale.entity.Product;
import com.example.flashsale.entity.Review;
import com.example.flashsale.entity.User;
import com.example.flashsale.repository.ProductRepository;
import com.example.flashsale.repository.ReviewRepository;
import com.example.flashsale.repository.UserRepository;
import com.example.flashsale.util.PasswordUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class DataInitializer implements CommandLineRunner {

    private final ProductRepository productRepository;
    private final UserRepository userRepository;
    private final ReviewRepository reviewRepository;
    private final StringRedisTemplate redisTemplate;

    @Override
    public void run(String... args) {
        seedUsers();
        seedProducts();
        seedReviews();
    }

    private void seedReviews() {
        if (reviewRepository.count() == 0) {
            List<Review> defaultReviews = List.of(
                    Review.builder()
                            .productId(1L)
                            .userId(1L)
                            .userName("Trần Minh Tuấn")
                            .rating(5)
                            .comment("Săn được suất Flash Sale iPhone 16 Pro Max giá quá hời! Đóng gói cẩn thận, máy nguyên seal chuẩn VN/A, giao trong 2h.")
                            .createdAt(LocalDateTime.now().minusDays(1))
                            .build(),
                    Review.builder()
                            .productId(1L)
                            .userId(2L)
                            .userName("Lê Thu Hà (VIP)")
                            .rating(5)
                            .comment("Thanh toán VietQR cực kỳ mượt mà, áp voucher VIP10 giảm được hẳn 2 triệu. Hệ thống phản hồi tức thì không hề bị lag!")
                            .createdAt(LocalDateTime.now().minusHours(5))
                            .build(),
                    Review.builder()
                            .productId(2L)
                            .userId(1L)
                            .userName("Đặng Quốc Bảo")
                            .rating(5)
                            .comment("Tai nghe Sony WH-1000XM5 chống ồn đỉnh cao, đeo êm tai không bị đau đầu. TechShop CSKH rất nhiệt tình.")
                            .createdAt(LocalDateTime.now().minusDays(2))
                            .build()
            );
            reviewRepository.saveAll(defaultReviews);
            log.info(">> Đã khởi tạo đánh giá sản phẩm mẫu cho TechShop.");
        }
    }

    private void seedUsers() {
        if (userRepository.count() == 0) {
            log.info("Khởi tạo tài khoản người dùng mẫu...");

            User demoCustomer = User.builder()
                    .email("khachhang@techshop.vn")
                    .passwordHash(PasswordUtil.hashPassword("123456"))
                    .fullName("Nguyễn Hoàng Nam")
                    .phone("0988776655")
                    .address("123 Cầu Giấy, P. Dịch Vọng, Cầu Giấy, Hà Nội")
                    .role("USER")
                    .createdAt(LocalDateTime.now())
                    .build();

            User demoAdmin = User.builder()
                    .email("admin@techshop.vn")
                    .passwordHash(PasswordUtil.hashPassword("admin123"))
                    .fullName("Kỹ Sư Hệ Thống (Admin)")
                    .phone("0900112233")
                    .address("Tòa nhà TechShop Tower, Hà Nội")
                    .role("ADMIN")
                    .createdAt(LocalDateTime.now())
                    .build();

            userRepository.saveAll(List.of(demoCustomer, demoAdmin));
            log.info(">> Đã tạo tài khoản mẫu: [khachhang@techshop.vn / 123456] & [admin@techshop.vn / admin123]");
        }
    }

    private void seedProducts() {
        if (productRepository.count() == 0) {
            log.info("Khởi tạo danh mục sản phẩm công nghệ TechShop...");

            List<Product> defaultProducts = List.of(
                    Product.builder()
                            .name("iPhone 16 Pro Max 256GB - Titan Tự Nhiên")
                            .category("Điện thoại")
                            .price(19990000L)
                            .originalPrice(34990000L)
                            .stock(50)
                            .imageUrl("https://images.unsplash.com/photo-1695048133142-1a20484d2569?auto=format&fit=crop&w=800&q=80")
                            .description("Siêu phẩm Flagship Apple chip A18 Pro, khung vỏ Titan Cấp 5 siêu bền nhẹ, màn hình Super Retina XDR 6.9 inch 120Hz.")
                            .build(),
                    Product.builder()
                            .name("Sony WH-1000XM5 - Tai Nghe Chống Ồn Flagship")
                            .category("Âm thanh")
                            .price(6490000L)
                            .originalPrice(8490000L)
                            .stock(100)
                            .imageUrl("https://images.unsplash.com/photo-1505740420928-5e560c06d30e?auto=format&fit=crop&w=800&q=80")
                            .description("Đỉnh cao chống ồn chủ động ANC với 8 micro, thời lượng pin 30 giờ, chuẩn âm thanh Hi-Res Audio không dây LDAC.")
                            .build(),
                    Product.builder()
                            .name("MacBook Air M3 15-inch 16GB/512GB Space Gray")
                            .category("Laptop")
                            .price(28990000L)
                            .originalPrice(32990000L)
                            .stock(80)
                            .imageUrl("https://images.unsplash.com/photo-1517336714731-489689fd1ca8?auto=format&fit=crop&w=800&q=80")
                            .description("Thiết kế siêu mỏng nhẹ thời thượng, chip Apple M3 cực mạnh mẽ hỗ trợ 2 màn hình ngoài, pin lên đến 18 tiếng liên tục.")
                            .build(),
                    Product.builder()
                            .name("Apple Watch Ultra 2 GPS + Cellular 49mm Titanium")
                            .category("Đồng hồ thông minh")
                            .price(17490000L)
                            .originalPrice(21990000L)
                            .stock(60)
                            .imageUrl("https://images.unsplash.com/photo-1523275335684-37898b6baf30?auto=format&fit=crop&w=800&q=80")
                            .description("Màn hình sáng nhất từng có 3000 nits, định vị GPS tần số kép chuẩn xác, chuẩn kháng nước độ sâu 100 mét.")
                            .build(),
                    Product.builder()
                            .name("Bàn Phím Cơ Không Dây ASUS ROG Azoth OLED 75%")
                            .category("Gaming Gear")
                            .price(4890000L)
                            .originalPrice(5990000L)
                            .stock(120)
                            .imageUrl("https://images.unsplash.com/photo-1587829741301-dc798b83add3?auto=format&fit=crop&w=800&q=80")
                            .description("Màn hình hiển thị OLED trực quan, Gasket mount 3 lớp đệm gõ cực êm, Switch ROG NX đã lube sẵn từ nhà máy.")
                            .build(),
                    Product.builder()
                            .name("Máy Chơi Game Sony PlayStation 5 Slim 1TB")
                            .category("Gaming Gear")
                            .price(11990000L)
                            .originalPrice(13990000L)
                            .stock(40)
                            .imageUrl("https://images.unsplash.com/photo-1606813907291-d86efa9b94db?auto=format&fit=crop&w=800&q=80")
                            .description("Đồ họa đỉnh cao Ray Tracing 4K 120Hz, tay cầm DualSense phản hồi xúc giác sống động, ổ cứng SSD siêu tốc 1TB.")
                            .build()
            );

            productRepository.saveAll(defaultProducts);
        }

        // Nạp và Warm Cache tồn kho lên Redis cho toàn bộ sản phẩm
        List<Product> products = productRepository.findAll();
        for (Product p : products) {
            String redisStockKey = "product:stock:" + p.getId();
            String currentRedisStock = redisTemplate.opsForValue().get(redisStockKey);
            if (currentRedisStock == null) {
                redisTemplate.opsForValue().set(redisStockKey, String.valueOf(p.getStock()));
            }
            log.info(">> Warm Cache Tồn Kho Sản Phẩm: ID = {}, Tên = '{}', Tồn = {}",
                    p.getId(), p.getName(), p.getStock());
        }
    }
}
