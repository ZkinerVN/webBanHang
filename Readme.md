# ⚡ TechShop - High-Concurrency Flash Sale & E-Commerce Platform

**TechShop** là hệ sinh thái sàn thương mại điện tử thiết bị công nghệ chính hãng và săn deal Flash Sale chịu tải cao được xây dựng theo kiến trúc **Modular Monolith** kết hợp **Event-Driven Architecture** chuẩn công nghiệp. Hệ thống giải quyết trọn vẹn bài toán giao dịch đồng thời quy mô cực lớn (**High Concurrency**): hàng nghìn người dùng cùng tranh mua số lượng máy có hạn trong khung giờ vàng mà **tuyệt đối không bán âm kho (Zero Overselling)**, loại bỏ triệt để nghẽn kết nối Database, chống spam bot và bảo đảm tính toàn vẹn dữ liệu.

Dự án tích hợp đầy đủ một Website Bán hàng thực tế (Storefront), Đăng nhập/Đăng ký phân quyền (RBAC), Chọn biến thể động (Màu sắc & Dung lượng), Giỏ hàng đa sản phẩm với cơ chế Rollback kho nguyên tử, Cổng thanh toán VietQR Webhook tự động, Động cơ Voucher, Hóa đơn VAT điện tử, Hệ thống đánh giá 5 sao, Hộp thư thông báo thời gian thực qua Server-Sent Events (SSE), và **Bảng Điều Hành Quản Trị Hệ Thống & Chaos Engineering Studio** (`/admin.html`).

---

## 🏛️ Kiến Trúc Hệ Thống (System Architecture)

```
                            [Khách Hàng / Trình Duyệt / k6 Load Testing]
                                                │
                                                ▼ (HTTP REST / SSE Stream)
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                          Spring Boot Application (Port 8080)                           │
│                                                                                        │
│   ┌───────────────────────────────┐                  ┌──────────────────────────────┐  │
│   │     RateLimitInterceptor      │                  │    IdempotencyInterceptor    │  │
│   │   (Sliding Window Lua / 429)  │                  │   (Redis SETNX Token / 409)  │  │
│   └───────────────┬───────────────┘                  └──────────────┬───────────────┘  │
│                   └────────────────────────┬────────────────────────┘                  │
│                                            ▼                                           │
│                       ┌───────────────────────────────────────────┐                    │
│                       │   Circuit Breaker Guard (Resilience)      │                    │
│                       │   (State: CLOSED | OPEN | HALF_OPEN / 503)│                    │
│                       └────────────────────┬──────────────────────┘                    │
│                                            ▼                                           │
│         [FlashSaleController / AuthController / PaymentController / ReviewController]  │
│                                            │                                           │
│                                            ▼                                           │
│                 [FlashSaleService / SseService / CircuitBreakerService]                │
└───────────────────────┬────────────────────────────┬───────────────────────────────────┘
                        │                            │
    1. Atomic Decr Lua  │                            │ 2. Publish Async Events
    (stock_decrement)   ▼                            ▼
      ┌─────────────────┐             ┌───────────────────────────────────────────┐
      │   Redis 7 Cache │             │           RabbitMQ Message Broker         │
      │ - Lua Stock Decr│             │ ┌───────────────────────────────────────┐ │
      │ - Rate Limit ZSet             │ │ flashsale.order.queue (Main Queue)    │ │
      │ - Session Tokens│             │ └──────────────────┬────────────────────┘ │
      │ - SSE Broadcast │             │ ┌──────────────────▼────────────────────┐ │
      └────────┬────────┘             │ │ flashsale.delay.queue (TTL 30 Giây)   │ │
               │                      │ └──────────────────┬────────────────────┘ │
               │                      │ ┌──────────────────▼────────────────────┐ │
               │                      │ │ flashsale.dlq.queue (Dead Letter/Revert)│
               │                      │ └──────────────────┬────────────────────┘ │
               │                      │ ┌──────────────────▼────────────────────┐ │
               │                      │ │ flashsale.notification.queue (In-App) │ │
               │                      │ └───────────────────────────────────────┘ │
               │                      └────────────────────┬──────────────────────┘
               │ 5. Bồi hoàn kho (Timeout/Lỗi)             │
               │    Revert Stock                           │ Consume (Async Pipeline)
               ▼                                           ▼
      ┌─────────────────────────────────────────────────────────────┐
      │                        OrderConsumer                        │
      │        - Transactional CAS Write DB                         │
      │        - Auto-Timeout Cancellation & Stock Rollback         │
      └──────────────────────────────┬──────────────────────────────┘
                                     │
                                     ▼ (Atomic CAS Query: stock >= :qty)
                      ┌─────────────────────────────────────────────┐
                      │             PostgreSQL 16 Database          │
                      │  - products (CAS Optimistic Stock Guard)    │
                      │  - orders (Unique IdempotencyKey Constraint)│
                      │  - users (Salted SHA-256 Auth & Roles)      │
                      │  - reviews (Verified Purchase Ratings)      │
                      │  - notifications (System & Order Alerts)    │
                      └─────────────────────────────────────────────┘
```

---

## 💡 Các Bài Toán Kỹ Thuật Cốt Lõi Đã Giải Quyết

### 1. Chống bán âm kho bằng Redis Lua Script (Atomic Decrement)
- **Vấn đề:** Khi hàng ngàn request đồng thời đọc tồn kho $Stock = 1$ ở cùng một mili-giây, mọi luồng đều thấy còn hàng và cùng trừ kho dẫn đến bán âm nghiêm trọng ($Stock < 0$).
- **Giải pháp:** Sử dụng tập lệnh Lua thực thi đơn luồng nguyên tử trên Redis server (`stock_decrement.lua`). Kiểm tra số lượng khả dụng và trừ kho tức thì trong 1 chu kỳ CPU của Redis, không tiêu tốn tài nguyên khóa phân tán (Distributed Lock overhead) và phản hồi Fast-fail **400 Bad Request** cho các request đến muộn khi hết hàng.

### 2. Khả năng chịu lỗi với Circuit Breaker & Fallback Mode (Resilience Pattern)
- **Vấn đề:** Khi cơ sở dữ liệu hoặc hệ thống downstream gặp sự cố quá tải, các request dồn ứ sẽ làm cạn kiệt tài nguyên máy chủ (thread pool exhaustion) gây sập toàn bộ ứng dụng (Cascading Failure).
- **Giải pháp:** Tích hợp `CircuitBreakerService` tự xây dựng chuẩn State Machine (`CLOSED`, `OPEN`, `HALF_OPEN`) với thuật toán Sliding Window (theo dõi 10 request gần nhất, ngưỡng lỗi 50%, thời gian ngắt 10 giây). Khi mạch ở trạng thái `OPEN`, hệ thống lập tức kích hoạt **Fallback Mode (HTTP 503 Service Unavailable)** bảo vệ toàn diện kho và DB, đồng thời hiển thị cảnh báo thân thiện trên giao diện người dùng. Có thể kiểm thử trực quan qua *Admin Chaos Studio*.

### 3. Đồng bộ tồn kho thời gian thực qua Server-Sent Events (SSE)
- **Vấn đề:** Cơ chế Polling (trình duyệt liên tục gửi request 1s/lần) làm lãng phí băng thông và gây nghẽn máy chủ khi có hàng ngàn người đang mở trang web.
- **Giải pháp:** Xây dựng `SseService` phát luồng dữ liệu một chiều từ máy chủ tới toàn bộ client (`/api/v1/flash-sale/sse/stock-stream`). Bất cứ khi nào Redis trừ kho hoặc bồi hoàn kho thành công, số lượng tồn kho mới được đẩy ngay lập tức về giao diện người dùng với độ trễ dưới 5ms.

### 4. Đặt hàng giỏ hàng đa sản phẩm với Rollback nguyên tử (All-or-Nothing Batch)
- **Vấn đề:** Khách hàng thanh toán giỏ hàng gồm 3 sản phẩm khác nhau. Nếu sản phẩm thứ 3 hết hàng sau khi đã trừ kho 2 sản phẩm đầu, việc trừ kho dở dang sẽ làm sai lệch dữ liệu kho và gián đoạn đơn hàng.
- **Giải pháp:** Endpoint `/api/v1/flash-sale/cart-checkout` thực hiện trừ kho tuần tự trên Redis Lua; nếu phát hiện bất kỳ sản phẩm nào không đủ tồn kho, hệ thống tự động kích hoạt **Compensating Transaction (Hoàn kho nguyên tử)** cho toàn bộ các sản phẩm đã trừ trước đó, bảo toàn 100% giỏ hàng và trả về thông báo rõ ràng cho khách hàng.

### 5. Cơ chế chống trùng lặp đơn hàng (Idempotency Key Pattern)
- **Vấn đề:** Khách hàng click liên tiếp nút Mua do mạng chập chờn hoặc do retry tự động từ client/gateway, gây trùng lặp đơn hàng và trừ tiền nhiều lần.
- **Giải pháp:** Sử dụng `Idempotency-Key` (UUID) qua HTTP Header. `IdempotencyInterceptor` sử dụng lệnh `SETNX` trên Redis với TTL 10 phút. Bất kỳ request nào đến cùng mã khi lượt trước đang xử lý đều bị chặn lập tức bằng mã **409 Conflict**.

### 6. Xả tải ghi Database bằng Message Queue (Async Order Pipeline)
- **Vấn đề:** Hàng nghìn lượt ghi chọc thẳng vào PostgreSQL cùng lúc gây cạn kiệt Connection Pool (HikariCP Starvation), làm đơ hệ thống.
- **Giải pháp:** Controller chỉ trừ kho trên Redis rồi đóng gói sự kiện bắn vào RabbitMQ (`flashsale.order.queue`) và phản hồi **202 Accepted** trong vòng 15ms. Phía sau, worker pool (`OrderConsumer`) tiêu thụ message và ghi nhận vào PostgreSQL theo tốc độ ổn định mà phần cứng chịu được.

### 7. Tự động thu hồi đơn hàng sau 30 giây bằng RabbitMQ Delay Queue
- **Vấn đề:** Khách hàng giữ hàng Flash Sale nhưng không tiến hành thanh toán, khiến sản phẩm bị găm giữ và khách hàng khác không thể mua.
- **Giải pháp:** Đơn hàng khởi tạo ở trạng thái `PENDING`. Delay Queue định tuyến message với TTL = 30s. Khi hết hạn, message tự động chuyển sang hàng đợi xử lý quá hạn. Worker kiểm tra nếu đơn vẫn chưa chuyển sang `SUCCESS`, hệ thống dùng câu lệnh CAS SQL để chuyển sang `EXPIRED`, đồng thời hoàn trả số lượng máy về cả PostgreSQL lẫn Redis Lua.

### 8. Chống Tool / Bot săn deal với Sliding Window Rate Limiter & Slide Captcha
- **Vấn đề:** Hacker sử dụng phần mềm tự động (Script/Bot) gửi hàng trăm request mỗi giây để gom toàn bộ máy giá rẻ.
- **Giải pháp:**
  - **Phía Backend:** `RateLimitInterceptor` kiểm tra tần suất theo `userId` bằng Redis ZSET (`rate_limit_sliding_window.lua`), chặn đứng mọi hành vi vượt quá 5 request/10 giây bằng mã **429 Too Many Requests**.
  - **Phía Frontend:** Tích hợp thanh trượt xác thực cử chỉ người dùng (**Anti-Bot Slide Verification**) trước khi gửi request đặt hàng Flash Sale.

---

## 🛒 Trải Nghiệm Mua Sắm Thực Tế (E-Commerce Features)

1. **Chọn Biến Thể Động (Dynamic Variants):**
   - **4 Tùy chọn Màu sắc Titan:** Titan Tự Nhiên (`#9e978e`), Titan Sa Mạc (`#e3d7c9`), Titan Đen (`#3c3d3a`), Titan Trắng (`#e2e4e1`) với hiệu ứng đổi hình ảnh mượt mà.
   - **3 Cấp Dung lượng Bộ nhớ:** 256GB (19.990.000đ), 512GB (23.990.000đ), 1TB (28.990.000đ) cập nhật giá Flash Sale, giá gốc, mức tiết kiệm và mã VietQR tương ứng.
   - Giỏ hàng hỗ trợ lưu trữ độc lập nhiều biến thể khác nhau (`cartKey`) mà không bị ghi đè.

2. **Giỏ Hàng & Mua Ngay Linh Hoạt:**
   - Drawer giỏ hàng trượt từ cạnh phải, hỗ trợ tăng giảm số lượng, xóa sản phẩm, và tính tổng phụ realtime.
   - Nút **MUA NGAY (30S)** khóa giữ suất Flash Sale ưu đãi kèm bộ đếm ngược 30 giây.

3. **Cổng Thanh Toán VietQR Tự Động & Mô Phỏng Webhook Ngân Hàng:**
   - Tạo mã VietQR chuẩn NAPAS chứa chính xác số tiền sau khi áp dụng voucher và nội dung chuyển khoản `TV {idempotencyKey}`.
   - Tích hợp nút **⚡ Mô Phỏng Chuyển Khoản Ngân Hàng (Webhook)** để kiểm tra quy trình thanh toán tự động chuyển đổi trạng thái sang `SUCCESS` và bắn pháo hoa confetti.
   - Cơ chế Live Auto-Detection tự động thăm dò và hoàn tất đơn hàng mà không cần tải lại trang.

4. **Động Cơ Khuyến Mãi / Voucher Engine:**
   - `TECH50K`: Giảm ngay 50.000đ cho đơn hàng từ 500.000đ.
   - `FREESHIP`: Miễn phí vận chuyển toàn quốc (-30.000đ).
   - `VIP10`: Giảm 10% (tối đa 2.000.000đ) cho thành viên TechShop VIP.

5. **Xuất Hóa Đơn VAT Điện Tử Chuẩn A4:**
   - Xem và in phiếu xuất kho kiêm Hóa đơn GTGT Điện tử trực tiếp từ danh sách đơn hàng.
   - Tự động bóc tách thuế suất GTGT 8%, thông tin pháp nhân CTCP Công Nghệ TechShop Việt Nam, mã số thuế và mã QR tra cứu.

6. **Đánh Giá & Xếp Hạng Sản Phẩm (Reviews & Ratings):**
   - Xem điểm đánh giá trung bình và các nhận xét từ người mua thực tế.
   - Gửi đánh giá từ 1 đến 5 sao kèm huy hiệu **✓ Đã mua hàng (Verified Purchase)**.

7. **Hộp Thư Thông Báo Cá Nhân (Notification Center):**
   - Hộp thư chuông thông báo trên thanh điều hướng nhận tin nhắn tự động khi giữ hàng thành công, thanh toán thành công hoặc đơn hàng hết hạn.
   - Hỗ trợ đánh dấu đã đọc từng thông báo hoặc toàn bộ.

8. **Bảng Điều Hành Quản Trị Hệ Thống & Chaos Studio (`/admin.html`):**
   - Biểu đồ trực quan doanh thu theo giờ và phân bổ trạng thái đơn hàng (sử dụng Chart.js).
   - Thống kê doanh thu thời gian thực, số đơn thành công, số đơn bị thu hồi do quá hạn.
   - Bảng quản lý đơn hàng toàn diện kèm bộ lọc trạng thái và chức năng Xuất file CSV (Excel).
   - Nạp thêm tồn kho (Stock Replenishment) cho cả Redis & PostgreSQL chỉ với 1 click.
   - **Chaos Engineering Studio:** Bắn tải kiểm thử 4 kịch bản khắc nghiệt (Spam Rate Limit, Đua 25 Luồng Đồng Thời, Gửi Trùng Idempotency Key, Cưỡng Chế Ngắt/Đóng Circuit Breaker).

---

## 🛠️ Công Nghệ Sử Dụng (Tech Stack)

| Thành phần | Công nghệ lựa chọn | Vai trò & Mục đích |
|---|---|---|
| **Backend Framework** | Java 21, Spring Boot 3.x / 4.x | RESTful API, DI/IoC, xử lý nghiệp vụ chịu tải cao |
| **Database** | PostgreSQL 16 | Lưu trữ quan hệ ACID, hỗ trợ Check Constraints và Row-level CAS lock |
| **ORM / Persistence** | Spring Data JPA, Hibernate ORM | Ánh xạ thực thể, Modifying queries, quản lý transaction |
| **In-Memory Cache** | Redis 7 + Redis Lua Scripts | Fast-fail cache, trừ kho nguyên tử, Sliding-window Rate Limiter, SSE broadcast |
| **Message Broker** | RabbitMQ 3.13 (AMQP) | Xếp hàng bất đồng bộ, Delay Queue (TTL 30s), Dead Letter Queue (DLQ), Notification Queue |
| **Resilience** | Circuit Breaker Pattern | Bảo vệ quá tải, chống sập chuỗi, Fallback Mode 503 |
| **Realtime Stream** | Server-Sent Events (SSE) | Đẩy biến động tồn kho tức thì về client không cần polling |
| **Frontend UI** | HTML5, Tailwind CSS, Vanilla JS, Chart.js, Confetti | Giao diện Cyber Dark hiện đại, Admin Portal & Chaos Studio |
| **API Documentation** | Springdoc OpenAPI 3 (Swagger UI) | Tài liệu hóa và kiểm thử trực quan toàn bộ REST API |
| **Testing** | JUnit 5, Mockito, k6 Benchmark | Kiểm thử đa luồng CAS và đo tải 500-1000 Virtual Users |
| **Containerization** | Docker, Docker Compose | Đóng gói hạ tầng PostgreSQL, Redis, RabbitMQ đồng bộ |

---

## 📂 Cấu Trúc Thư Mục (Project Structure)

```
flashsale/
├── docker-compose.yml              # Khởi tạo PostgreSQL 16, Redis 7, RabbitMQ 3.13
├── k6-flashsale-test.js            # Kịch bản k6 benchmark kiểm thử 500 - 1000 Virtual Users
├── pom.xml                         # Cấu hình dependencies Maven
└── src/
    ├── main/
    │   ├── java/com/example/flashsale/
    │   │   ├── FlashsaleApplication.java         # Điểm khởi chạy ứng dụng Spring Boot
    │   │   ├── config/
    │   │   │   ├── OpenApiConfig.java            # Cấu hình Swagger / OpenAPI 3 UI
    │   │   │   ├── RabbitMQConfig.java           # Định nghĩa 4 Queues: Main, Delay, DLQ, Notification
    │   │   │   ├── RedisConfig.java              # Cấu hình RedisTemplate & Lua Scripts
    │   │   │   └── WebMvcConfig.java             # Đăng ký Interceptors (RateLimit, Idempotency)
    │   │   ├── consumer/
    │   │   │   ├── NotificationConsumer.java     # Nhận sự kiện đơn hàng và tạo thông báo in-app
    │   │   │   └── OrderConsumer.java            # Tiêu thụ hàng đợi ghi DB + Delay Queue 30s timeout
    │   │   ├── controller/
    │   │   │   ├── AuthController.java           # Đăng nhập, đăng ký, lấy thông tin cá nhân
    │   │   │   ├── FlashSaleController.java      # Mua hàng, giỏ hàng, SSE stream, Circuit Breaker endpoints
    │   │   │   ├── GlobalExceptionHandler.java   # Xử lý tập trung mã lỗi HTTP 400, 404, 500
    │   │   │   ├── NotificationController.java   # Đọc, đánh dấu đã đọc thông báo người dùng
    │   │   │   ├── PaymentController.java        # Webhook ngân hàng & mô phỏng thanh toán VietQR
    │   │   │   ├── ReviewController.java         # Lấy và gửi đánh giá sản phẩm 5 sao
    │   │   │   └── VoucherController.java        # Kiểm tra và áp dụng mã giảm giá
    │   │   ├── dto/
    │   │   │   ├── AuthResponse.java             # Thông tin trả về sau xác thực
    │   │   │   ├── CartCheckoutRequest.java      # DTO đặt giỏ hàng đa sản phẩm
    │   │   │   ├── LoginRequest.java             # DTO đăng nhập
    │   │   │   ├── NotificationMessage.java      # Message gửi qua RabbitMQ thông báo
    │   │   │   ├── OrderMessage.java             # Message đặt hàng qua RabbitMQ
    │   │   │   ├── PaymentWebhookRequest.java    # Payload webhook từ cổng thanh toán ngân hàng
    │   │   │   ├── PurchaseRequest.java          # DTO mua hàng Flash Sale trực tiếp
    │   │   │   └── RegisterRequest.java          # DTO đăng ký tài khoản
    │   │   ├── entity/
    │   │   │   ├── Notification.java             # Bảng notifications trong DB
    │   │   │   ├── Order.java                    # Bảng orders (lưu vết IdempotencyKey, số tiền, địa chỉ)
    │   │   │   ├── OrderStatus.java              # Trạng thái: PENDING, SUCCESS, EXPIRED, FAILED
    │   │   │   ├── Product.java                  # Bảng products (tồn kho, đơn giá, hình ảnh)
    │   │   │   ├── Review.java                   # Bảng reviews (điểm đánh giá, nhận xét)
    │   │   │   └── User.java                     # Bảng users (email, mật khẩu salt, quyền hạn)
    │   │   ├── initializer/
    │   │   │   └── DataInitializer.java          # Tự động seed tài khoản mẫu, sản phẩm & Warm Cache
    │   │   ├── interceptor/
    │   │   │   ├── IdempotencyInterceptor.java   # Chặn trùng lặp đơn hàng qua Redis SETNX
    │   │   │   └── RateLimitInterceptor.java     # Chặn spam bot qua Redis Sliding Window ZSET
    │   │   ├── repository/
    │   │   │   ├── NotificationRepository.java   # JPA repository cho thông báo
    │   │   │   ├── OrderRepository.java          # JPA repository cho đơn hàng & doanh thu
    │   │   │   ├── ProductRepository.java        # JPA repository cho sản phẩm & CAS query
    │   │   │   ├── ReviewRepository.java         # JPA repository cho đánh giá
    │   │   │   └── UserRepository.java           # JPA repository cho người dùng
    │   │   ├── service/
    │   │   │   ├── AuthService.java              # Xác thực người dùng và cấp phát Token Redis
    │   │   │   ├── CircuitBreakerService.java    # Quản lý trạng thái mạch CLOSED / OPEN / HALF_OPEN
    │   │   │   ├── FlashSaleService.java         # Nghiệp vụ trừ kho Lua, RabbitMQ pipeline, hoàn kho
    │   │   │   └── SseService.java               # Đẩy biến động tồn kho realtime tới người dùng
    │   │   └── util/
    │   │       └── PasswordUtil.java             # Mã hóa mật khẩu an toàn với SHA-256 + Salt
    │   └── resources/
    │       ├── application.yml                   # Cấu hình DataSource, Redis, RabbitMQ, HikariCP
    │       ├── scripts/
    │       │   ├── stock_decrement.lua           # Script Redis trừ tồn kho nguyên tử
    │       │   └── rate_limit_sliding_window.lua # Script Redis giới hạn tần suất trượt
    │       └── static/
    │           ├── index.html                    # Giao diện Storefront TechShop & săn deal Flash Sale
    │           └── admin.html                    # Bảng điều hành Quản trị, Chart.js & Chaos Studio
    └── test/
        └── java/com/example/flashsale/
            ├── FlashSaleConcurrencyTest.java     # Kiểm thử CAS 200 luồng đồng thời không bán âm
            └── FlashsaleApplicationTests.java    # Kiểm thử khởi động Spring Boot Context
```

---

## 🚀 Hướng Dẫn Khởi Chạy (Quickstart)

### Yêu cầu môi trường
- **Docker & Docker Compose** (đã được bật).
- **JDK 17** hoặc **JDK 21** trở lên.
- Maven 3.8+ (hoặc dùng sẵn tệp wrapper `./mvnw` đi kèm).

---

### Bước 1: Khởi động Hạ tầng Docker (PostgreSQL, Redis, RabbitMQ)
Chạy lệnh sau tại thư mục gốc dự án:
```bash
docker compose up -d
```

Kiểm tra trạng thái cả 3 container đang hoạt động bình thường:
```bash
docker compose ps
```
- **RabbitMQ Management Dashboard:** [http://localhost:15672](http://localhost:15672) *(User: `guest` / Pass: `guest`)*
- **Redis Port:** `6379`
- **PostgreSQL Port:** `5432` *(Database: `flashsale_db`, User: `postgres`, Pass: `password`)*

---

### Bước 2: Khởi chạy Ứng dụng Spring Boot
Chạy ứng dụng bằng Maven wrapper:

**Trên Windows PowerShell:**
```powershell
.\mvnw.cmd clean spring-boot:run
```

**Trên macOS / Linux:**
```bash
./mvnw clean spring-boot:run
```

Khi ứng dụng khởi động thành công, hệ thống tự động Seed dữ liệu mẫu và nạp sẵn tồn kho vào Redis:
```plaintext
>> Đã khởi tạo người dùng mẫu: khachhang@techshop.vn (VIP) & admin@techshop.vn (ADMIN)
>> Đã khởi tạo danh mục 6 sản phẩm công nghệ TechShop.
>> Đã nạp sẵn dữ liệu đánh giá 5 sao cho sản phẩm.
>> Đã Warm Cache thành công: Product ID = 1, Key [product:stock:1] = 50
```

---

### Bước 3: Trải Nghiệm Ứng Dụng Trên Trình Duyệt

1. **Cửa Hàng Mua Sắm TechShop:**
   👉 Truy cập: [http://localhost:8080](http://localhost:8080)
   - Đăng nhập bằng tài khoản mẫu: `khachhang@techshop.vn` / mật khẩu: `123456`.
   - Chọn biến thể màu sắc (Titan Sa Mạc, Titan Tự Nhiên,...) và dung lượng bộ nhớ (256GB, 512GB, 1TB).
   - Thêm vào giỏ hàng hoặc bấm **MUA NGAY (30S)** (kéo thanh trượt chống bot để kích hoạt).
   - Nhập mã voucher `VIP10` hoặc `TECH50K` để giảm giá.
   - Bấm nút **⚡ Mô Phỏng Chuyển Khoản Ngân Hàng** để kiểm tra Webhook duyệt đơn tự động.
   - Mở mục **Đơn Hàng** để xem tiến độ và in **Hóa Đơn Điện Tử VAT GTGT** chuẩn A4.

2. **Bảng Quản Trị Hệ Thống & Chaos Studio:**
   👉 Truy cập: [http://localhost:8080/admin.html](http://localhost:8080/admin.html)
   - Đăng nhập tài khoản Quản trị: `admin@techshop.vn` / mật khẩu: `admin123`.
   - Quan sát biểu đồ doanh thu Chart.js thời gian thực và quản lý đơn hàng.
   - Thử nghiệm các kịch bản trong **Chaos Studio**:
     - *Thẻ 1:* Spam Rate Limit (Kiểm tra mã lỗi HTTP 429).
     - *Thẻ 2:* Đua tranh 25 Users cùng lúc (Kiểm tra trừ kho Redis nguyên tử).
     - *Thẻ 3:* Gửi trùng Idempotency Key (Kiểm tra mã lỗi HTTP 409).
     - *Thẻ 4:* Ngắt mạch Circuit Breaker (Kiểm tra Fallback Mode HTTP 503 và phục hồi về CLOSED).

3. **Tài Liệu REST API Tương Tác (Swagger UI / OpenAPI 3):**
   👉 Truy cập: [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html)
   - Tra cứu đầy đủ các endpoint: Auth, Flash Sale, Giỏ hàng, SSE, Circuit Breaker, Đánh giá, Thông báo, Thanh toán.

4. **Giám Sát Hạ Tầng Sức Khỏe Ứng Dụng (Spring Actuator):**
   👉 Truy cập: [http://localhost:8080/actuator/health](http://localhost:8080/actuator/health)
   - Kiểm tra trạng thái sẵn sàng kết nối của PostgreSQL, Redis Cache và RabbitMQ.

---

## ⚡ Kiểm Thử Tải Trọng Cao Điểm Với k6 (Benchmark)

Dự án cung cấp sẵn tệp kịch bản kiểm thử tải `k6-flashsale-test.js` mô phỏng **500 - 1000 Virtual Users** cùng đổ dồn tranh mua 50 suất Flash Sale trong khung giờ vàng:

```bash
# Cài đặt k6 nếu chưa có (Windows: winget install k6 / macOS: brew install k6)
k6 run k6-flashsale-test.js
```

### Tiêu chuẩn nghiệm thu hiệu năng:
- ✅ **Zero Overselling:** Tổng số đơn hàng ghi nhận thành công trong cơ sở dữ liệu luôn $\le 50$ máy, không có bất kỳ đơn nào bị bán âm.
- ✅ **Zero 500 Server Error:** `techshop_unexpected_errors_5xx = 0`.
- ✅ **Ultra Low Latency:** 95% request mua hàng ban đầu phản hồi dưới 200ms nhờ cơ chế trừ kho Redis Lua Script và trả về HTTP 202 Accepted.
- ✅ **Anti-Spam & Bot Shield:** 100% các request vi phạm ngưỡng tần suất trượt bị chặn đứng bằng HTTP 429 Too Many Requests.
- ✅ **Circuit Breaker Resilience:** Tự động bảo vệ máy chủ khi quá tải, giữ an toàn tuyệt đối cho toàn bộ giỏ hàng và dữ liệu tồn kho.

---

## 📄 Bản Quyền & Giấy Phép
Dự án được phát triển nhằm mục đích nghiên cứu, học tập và triển khai thực tế các mô hình kiến trúc phần mềm chịu tải cao (High Concurrency & Fault Tolerance). Mọi quyền được bảo lưu bởi **TechShop Platform**.
