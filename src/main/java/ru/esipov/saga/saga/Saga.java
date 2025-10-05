package ru.esipov.saga.saga;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.job4j.core.command.*;
import ru.job4j.core.events.*;
import ru.job4j.core.sagadto.OrderStatus;

@Component
@KafkaListener(topics = {
        "${orders.events.topic.name}",
        "${products.events.topic.name}",
        "${payments.events.topic.name}"
})
public class Saga {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String productsCommandsTopicName;
    private final String paymentsCommandsTopicName;
    private final String ordersCommandsTopicName;

    public Saga(KafkaTemplate<String, Object> kafkaTemplate,
                @Value("${products.commands.topic.name}") String productsCommandsTopicName,
                @Value("${payments.command.topic.name}") String paymentsCommandsTopicName,
                @Value("${orders.commands.topic.name}") String ordersCommandsTopicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.productsCommandsTopicName = productsCommandsTopicName;
        this.paymentsCommandsTopicName = paymentsCommandsTopicName;
        this.ordersCommandsTopicName = ordersCommandsTopicName;
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderCreatedEvent event) {
        ReserveProductCommand command = new ReserveProductCommand(
                event.productId(), event.productQuantity(), event.orderId()
        );
        kafkaTemplate.send(productsCommandsTopicName, command);
        OrderHistoryCommand orderHistoryCommand = new OrderHistoryCommand(event.orderId(), OrderStatus.CREATED);
        kafkaTemplate.send(ordersCommandsTopicName, orderHistoryCommand);
    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservedEvent event) {
        ProcessPaymentCommand processPaymentCommand = new ProcessPaymentCommand(
                event.orderId(),
                event.productId(),
                event.productPrice(),
                event.productQuantity()
        );
        kafkaTemplate.send(paymentsCommandsTopicName, processPaymentCommand);

    }

    @KafkaHandler
    public void handleEvent(@Payload PaymentProcessedEvent event) {
        ApproveOrderCommand approveOrderCommand = new ApproveOrderCommand(
                event.orderId()
        );
        kafkaTemplate.send(ordersCommandsTopicName, approveOrderCommand);
    }

    @KafkaHandler
    public void handleEvent(@Payload OrderApprovedEvent event) {
        OrderHistoryCommand orderHistoryCommand = new OrderHistoryCommand(event.orderId(), OrderStatus.APPROVED);
        kafkaTemplate.send(ordersCommandsTopicName, orderHistoryCommand);
    }

    @KafkaHandler
    public void handleEvent(@Payload PaymentFailedEvent event) {
        CancelProductReservationCommand cancelProductReservationCommand = new CancelProductReservationCommand(
                event.orderId(),
                event.productId(),
                event.productQuantity()
        );
        kafkaTemplate.send(productsCommandsTopicName, cancelProductReservationCommand);
    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservationCancelledEvent event) {
        RejectOrderCommand rejectOrderCommand = new RejectOrderCommand(event.orderId());
        kafkaTemplate.send(ordersCommandsTopicName, rejectOrderCommand);
        OrderHistoryCommand orderHistoryCommand = new OrderHistoryCommand(event.orderId(), OrderStatus.REJECTED);
        kafkaTemplate.send(ordersCommandsTopicName, orderHistoryCommand);
    }

    @KafkaHandler
    public void handleEvent(@Payload ProductReservationFailedEvent event) {
        RejectOrderCommand rejectOrderCommand = new RejectOrderCommand(event.orderId());
        kafkaTemplate.send(ordersCommandsTopicName, rejectOrderCommand);
        OrderHistoryCommand orderHistoryCommand = new OrderHistoryCommand(event.orderId(), OrderStatus.REJECTED);
        kafkaTemplate.send(ordersCommandsTopicName, orderHistoryCommand);
    }
}
