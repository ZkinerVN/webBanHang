package com.example.flashsale.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    // 1. Luồng xử lý đơn hàng chính
    public static final String EXCHANGE_NAME = "flashsale.order.exchange";
    public static final String QUEUE_NAME = "flashsale.order.queue";
    public static final String ROUTING_KEY = "flashsale.order.key";

    // 2. Dead Letter Queue (Hứng message khi Worker ghi DB gặp lỗi)
    public static final String DLX_EXCHANGE = "flashsale.order.dlx";
    public static final String DLQ_NAME = "flashsale.order.dlq";
    public static final String DLQ_ROUTING_KEY = "flashsale.order.dlq.key";

    // 3. Luồng Delay Queue kiểm tra hết hạn thanh toán (TTL 30 giây)
    public static final String DELAY_QUEUE = "flashsale.order.delay.queue";
    public static final String DELAY_ROUTING_KEY = "flashsale.order.delay.key";
    public static final String TIMEOUT_QUEUE = "flashsale.order.timeout.queue";
    public static final String TIMEOUT_ROUTING_KEY = "flashsale.order.timeout.key";

    @Bean
    public DirectExchange orderExchange() {
        return new DirectExchange(EXCHANGE_NAME);
    }

    @Bean
    public DirectExchange dlxExchange() {
        return new DirectExchange(DLX_EXCHANGE);
    }

    // Queue chính gắn sẵn DLX khi xảy ra lỗi (Reject/Nack)
    @Bean
    public Queue orderQueue() {
        return QueueBuilder.durable(QUEUE_NAME)
                .withArgument("x-dead-letter-exchange", DLX_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", DLQ_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding orderBinding() {
        return BindingBuilder.bind(orderQueue()).to(orderExchange()).with(ROUTING_KEY);
    }

    // Queue chứa các message bị lỗi (DLQ)
    @Bean
    public Queue deadLetterQueue() {
        return QueueBuilder.durable(DLQ_NAME).build();
    }

    @Bean
    public Binding dlqBinding() {
        return BindingBuilder.bind(deadLetterQueue()).to(dlxExchange()).with(DLQ_ROUTING_KEY);
    }

    // Queue giữ chỗ (Không có Consumer đọc, sau 30 giây tự chuyển sang TIMEOUT_QUEUE)
    @Bean
    public Queue delayQueue() {
        return QueueBuilder.durable(DELAY_QUEUE)
                .withArgument("x-message-ttl", 30000) // 30 giây (Thực tế để 300000 = 5 phút)
                .withArgument("x-dead-letter-exchange", DLX_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", TIMEOUT_ROUTING_KEY)
                .build();
    }

    @Bean
    public Binding delayBinding() {
        return BindingBuilder.bind(delayQueue()).to(orderExchange()).with(DELAY_ROUTING_KEY);
    }

    // Queue xử lý thu hồi vé khi hết hạn
    @Bean
    public Queue timeoutQueue() {
        return QueueBuilder.durable(TIMEOUT_QUEUE).build();
    }

    @Bean
    public Binding timeoutBinding() {
        return BindingBuilder.bind(timeoutQueue()).to(dlxExchange()).with(TIMEOUT_ROUTING_KEY);
    }

    @Bean
    public Jackson2JsonMessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }
}