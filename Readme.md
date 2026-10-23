🎟️ Ticketis - High-Concurrency FlashSale & Ticket Booking EngineTicketVibe là hệ thống săn deal và đặt vé sự kiện chịu tải cao được xây dựng theo kiến trúc Modular Monolith chuẩn công nghiệp. Hệ thống được thiết kế để giải quyết bài toán giao dịch đồng thời quy mô lớn (High Concurrency): hàng nghìn người cùng tranh mua một số lượng vé giới hạn trong một khung giờ mà tuyệt đối không bán âm kho (Zero Overselling), chống nghẽn kết nối Database và bảo vệ tính toàn vẹn dữ liệu.Dự án tích hợp sẵn một Website Thương mại Điện tử thực tế kèm Bảng Điều Khiển Kiểm Thử Kỹ Thuật (Chaos & Concurrency Studio) để biểu diễn trực quan các kịch bản nghẽn mạng ngay trên trình duyệt.🏛️ Kiến Trúc Hệ Thống (System Architecture)                            [Client / Web UI / k6 / JMeter]
│
▼ (HTTP REST / JSON)
┌───────────────────────────────────────────────────────────────────┐
│                 Spring Boot Application (Port 8080)               │
│                                                                   │
│  ┌───────────────────────┐             ┌───────────────────────┐  │
│  │ RateLimitInterceptor  │             │ IdempotencyInterceptor│  │
│  │ (Redis Counter / 429) │             │ (Redis SETNX / 409)   │  │
│  └───────────┬───────────┘             └───────────┬───────────┘  │
│              └──────────────────┬──────────────────┘              │
│                                 ▼                                 │
│                     [FlashSaleController]                         │
│                                 │                                 │
│                                 ▼                                 │
│                      [FlashSaleService]                           │
└────────────────────────┬──────────────────┬───────────────────────┘
│                  │
1. Atomic Decr   │                  │ 2. Publish Async Events
(Lua Script)     ▼                  ▼
┌─────────────────┐    ┌─────────────────────────────────┐
│   Redis Cache   │    │            RabbitMQ             │
│ (In-Memory K/V) │    │ ┌─────────────────────────────┐ │
└─────────────────┘    │ │ flashsale.order.queue       │ │
▲             │ └──────────────┬──────────────┘ │
│             │ ┌──────────────▼──────────────┐ │
5. Revert Stock │             │ │ Delay Queue (TTL 30s)       │ │
(If unpaid/err) │             │ └──────────────┬──────────────┘ │
│             │ ┌──────────────▼──────────────┐ │
│             │ │ Dead Letter Queue (DLQ)     │ │
│             │ └─────────────────────────────┘ │
│             └────────────────┬────────────────┘
│                              │ Consume
│                              ▼
┌────────┴───────────────────────────────────────────────┐
│                    OrderConsumer                       │
│         (@Transactional DB Write + DLQ Handler)        │
└───────────────────────────────┬────────────────────────┘
│
▼ (ACID Transaction)
┌─────────────────────────────────┐
│      PostgreSQL Database        │
│   - products (Row-level Lock)   │
│   - orders (Unique Constraint)  │
└─────────────────────────────────┘
💡 Các Bài Toán Kỹ Thuật Cốt Lõi Đã Giải Quyết1. Chống bán âm kho bằng Redis Lua Script (Atomic Decrement)Vấn đề: Khi hàng trăm request cùng đọc tồn kho $Stock = 1$ ở cùng một mili-giây, cả hai đều thấy còn hàng và cùng trừ kho dẫn đến bán âm ($Stock < 0$).Giải pháp: Sử dụng Lua Script chạy đơn luồng nguyên tử trên Redis server. Kiểm tra điều kiện tồn kho và trừ trực tiếp trong một chu kỳ CPU của Redis, không cần tốn chi phí phân tán (Distributed Lock overhead) và phản hồi Fast-fail mã 400 cho 95% request dư thừa.2. Tính lặp API (Idempotency Key Pattern)Vấn đề: Khách hàng bấm nút Mua nhiều lần do mạng chập chờn gây trùng đơn và trừ tiền 2 lần.Giải pháp: Sử dụng Idempotency-Key (UUID) truyền qua HTTP Header. IdempotencyInterceptor sử dụng lệnh SETNX trên Redis với TTL 120s. Bất kỳ request thứ hai nào đến cùng mã khi lượt trước chưa xử lý xong đều lập tức bị từ chối bằng mã 409 Conflict.3. Xả tải ghi Database bằng Message Queue (Async Pipeline)Vấn đề: Hàng nghìn lượt ghi chọc thẳng vào PostgreSQL gây cạn kiệt Connection Pool (HikariCP Starvation) làm sập hệ thống.Giải pháp: Controller chỉ trừ kho tạm trên Redis rồi bắn sự kiện vào RabbitMQ và trả về mã 202 Accepted dưới 30ms. Phía sau, worker pool (concurrency = 5-10, prefetch = 10) kéo message ra ghi vào DB theo tốc độ tối ưu mà phần cứng chịu được.4. Thu hồi vé tự động bằng RabbitMQ Delay Queue (Order Timeout)Vấn đề: Khách hàng săn được vé thành công nhưng không thanh toán, khiến vé bị găm giữ và doanh nghiệp thất thoát doanh thu.Giải pháp: Đơn hàng khởi tạo mang trạng thái PENDING. Một message được đẩy vào Delay Queue có thời gian sống (TTL = 30s). Khi hết hạn, message tự động trượt sang timeout.queue. Worker kiểm tra nếu đơn vẫn chưa SUCCESS, lập tức chuyển sang EXPIRED và tự động bồi hoàn lại tồn kho cho cả PostgreSQL lẫn Redis.5. Khả năng chịu lỗi và Dead Letter Queue (DLQ)Toàn bộ thao tác ghi DB được bọc trong @Transactional(rollbackFor = Exception.class) kết hợp câu lệnh SQL an toàn:SQLUPDATE products SET stock = stock - :qty WHERE id = :id AND stock >= :qty;
Nếu có lỗi hệ thống hoặc database ngắt kết nối, message được ném sang Dead Letter Queue (DLQ) để retry có kiểm soát, đồng thời kích hoạt hàm bồi hoàn cộng trả lại vé trên Redis để tránh lệch dữ liệu giữa Cache và Storage.6. Giới hạn tần suất (Rate Limiting)RateLimitInterceptor kiểm tra tần suất theo userId (hoặc Client IP) bằng bộ đếm Redis Sliding-Window. Chặn đứng các hành vi dùng tool/bot spam quá 5 request/10 giây bằng mã 429 Too Many Requests.🛠️ Công Nghệ Sử Dụng (Tech Stack)Thành phầnCông nghệ lựa chọnMục đích sử dụngBackend FrameworkJava 17/21, Spring Boot 3.x / 4.xNền tảng cốt lõi xây dựng REST API và điều hướng nghiệp vụ.DatabasePostgreSQL 16Lưu trữ quan hệ ACID, hỗ trợ Check Constraints và Row-level Lock.ORM / PersistenceSpring Data JPA, Hibernate ORMThao tác thực thể, tối ưu chỉ mục và câu lệnh Modifying Queries.In-Memory CacheRedis 7 + Redis Lua ScriptFast-fail cache, trừ kho nguyên tử, bộ đếm Rate Limiting & Idempotency.Message BrokerRabbitMQ 3.13 (AMQP)Xếp hàng xử lý bất đồng bộ, Delay Queue (TTL) & Dead Letter Queue (DLQ).Frontend StorefrontTailwind CSS, HTML5, Vanilla JSGiao diện đặt vé thời gian thực, bảng kiểm thử giả lập tải (Chaos Studio).Testing & BenchmarkJUnit 5, CountDownLatch, k6Đo lường hiệu năng, chứng minh tính an toàn đa luồng.ContainerizationDocker, Docker ComposeĐóng gói và triển khai môi trường hạ tầng chỉ bằng 1 câu lệnh.📂 Cấu Trúc Thư Mục (Project Structure)Plaintextflashsale-engine/
├── docker-compose.yml              # Cấu hình PostgreSQL, Redis, RabbitMQ
├── pom.xml                         # Khai báo Maven dependencies
└── src/
├── main/
│   ├── java/com/example/flashsale/
│   │   ├── FlashsaleApplication.java
│   │   ├── config/             # Cấu hình Redis, RabbitMQ (DLQ/Delay), WebMvc
│   │   ├── consumer/           # Worker xử lý Queue chính & Timeout Queue
│   │   ├── controller/         # REST API bán vé, thanh toán, kiểm tra trạng thái
│   │   ├── dto/                # OrderMessage record payload
│   │   ├── entity/             # Product, Order, OrderStatus
│   │   ├── initializer/        # DataInitializer (Tự động Seed & Warm Cache)
│   │   ├── interceptor/        # RateLimitInterceptor & IdempotencyInterceptor
│   │   ├── repository/         # ProductRepository, OrderRepository
│   │   └── service/            # FlashSaleService (Trừ kho, Bồi hoàn, Giao dịch DB)
│   └── resources/
│       ├── application.yml     # Tham số kết nối & Pool Hikari
│       ├── scripts/
│       │   └── stock_decrement.lua # Script trừ tồn kho nguyên tử
│       └── static/
│           └── index.html      # Giao diện TicketVibe & Chaos Studio
└── test/
└── java/com/example/flashsale/
└── FlashSaleConcurrencyTest.java # JUnit test 200 luồng đồng thời
🚀 Hướng Dẫn Khởi Chạy (Quickstart)Yêu cầu tiên quyếtĐã cài đặt Docker Desktop (và đã khởi động).Đã cài đặt JDK 17 hoặc JDK 21.Maven 3.8+ (hoặc dùng sẵn ./mvnw đính kèm).Bước 1: Khởi động Hạ tầng Docker (PostgreSQL, Redis, RabbitMQ)Chạy lệnh sau tại thư mục gốc của dự án:Bashdocker compose up -d
Kiểm tra đảm bảo cả 3 dịch vụ đều ở trạng thái running:Bashdocker compose ps
RabbitMQ Dashboard: http://localhost:15672 (User: guest / Pass: guest)Redis Port: 6379PostgreSQL Port: 5432Bước 2: Thiết lập Check Constraint cho PostgreSQLĐể hỗ trợ đầy đủ các trạng thái đơn hàng (PENDING, SUCCESS, FAILED, EXPIRED), chạy lệnh cập nhật Constraint:Bashdocker exec -it flashsale-postgres psql -U postgres -d flashsale_db -c "ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_status_check; ALTER TABLE orders ADD CONSTRAINT orders_status_check CHECK (status IN ('PENDING', 'SUCCESS', 'FAILED', 'EXPIRED'));"
Bước 3: Khởi chạy Ứng dụng Spring BootChạy ứng dụng bằng Maven wrapper:Bash# Trên macOS / Linux:
./mvnw clean spring-boot:run

# Trên Windows PowerShell:
.\mvnw.cmd clean spring-boot:run
Khi ứng dụng khởi động xong, bạn sẽ thấy dòng log xác nhận đã tự động nạp dữ liệu và Warm Cache:Plaintext>> Đã Warm Cache thành công: Product ID = 1, Key [product:stock:1] = 50
🖥️ Trải Nghiệm Giao Diện TicketVibe & Chaos StudioMở trình duyệt web và truy cập vào:
👉 http://localhost:8080

