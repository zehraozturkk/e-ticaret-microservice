package com.turkcell.product_service.polling;

// Bu sınıf Debezium CDC ile değiştirildi.
// Debezium, outbox tablosuna yapılan her INSERT'i PostgreSQL WAL (Write-Ahead Log) üzerinden okur
// ve doğrudan Kafka'ya iletir. Polling'e gerek kalmaz.
//
// Connector kayıt dosyası: docker/debezium/outbox-connector.json
// Kayıt komutu (debezium-connect container ayaktayken):
//   curl -X POST http://localhost:8083/connectors \
//        -H "Content-Type: application/json" \
//        -d @docker/debezium/outbox-connector.json

// import java.time.Instant;
// import java.util.List;
//
// import org.springframework.cloud.stream.function.StreamBridge;
// import org.springframework.scheduling.annotation.Scheduled;
// import org.springframework.stereotype.Component;
//
// import com.turkcell.product_service.entity.OutboxEvent;
// import com.turkcell.product_service.entity.OutboxStatus;
// import com.turkcell.product_service.repository.OutboxRepository;
//
// import jakarta.transaction.Transactional;
//
//
// @Component
// public class OutboxPoller {
//
//     private final OutboxRepository outboxRepository;
//     private final StreamBridge streamBridge;
//
//     public OutboxPoller(OutboxRepository outboxRepository, StreamBridge streamBridge) {
//         this.outboxRepository = outboxRepository;
//         this.streamBridge = streamBridge;
//     }
//
//     @Scheduled(fixedDelay = 20000)
//     @Transactional
//     public void publishPendingEvents() {
//         List<OutboxEvent> events = outboxRepository.findPublishable(100);
//
//         for (OutboxEvent event : events) {
//             try {
//                 streamBridge.send(event.getEventType() + "-out-0", event.getPayload());
//                 event.setStatus(OutboxStatus.SENT);
//             } catch (Exception e) {
//                 if (event.getRetryCount() >= 3)
//                     event.setStatus(OutboxStatus.FAILED);
//                 else
//                     event.setRetryCount(event.getRetryCount() + 1);
//             }
//             event.setProcessedAt(Instant.now());
//             outboxRepository.save(event);
//         }
//     }
// }
