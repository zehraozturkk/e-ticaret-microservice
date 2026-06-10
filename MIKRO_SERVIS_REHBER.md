# Mikroservis Mimarisine Giriş Rehberi
> **Proje:** `microservices-spring` — Turkcell eğitim projesi  
> **Stack:** Spring Boot 4, Spring Cloud 2025, Kafka, PostgreSQL, Docker

---

## İçindekiler
1. [Mikroservis Nedir?](#1-mikroservis-nedir)
2. [Proje Genel Yapısı](#2-proje-genel-yapısı)
3. [pom.xml — Maven Yapısı](#3-pomxml--maven-yapısı)
4. [Her Servisin Katmanları](#4-her-servisin-katmanları)
5. [Enum Kullanımı](#5-enum-kullanımı)
6. [application.yml — Konfigürasyon](#6-applicationyml--konfigürasyon)
7. [docker-compose.yml — Altyapı](#7-docker-composeyml--altyapı)
8. [Eureka — Servis Keşfi](#8-eureka--servis-keşfi)
9. [Gateway — API Geçidi](#9-gateway--api-geçidi)
10. [Kafka — Olay Mesajlaşması](#10-kafka--olay-mesajlaşması)
11. [Outbox Pattern — Güvenilir Yayın](#11-outbox-pattern--güvenilir-yayın)
12. [Idempotency — Tekrar İşleme Güvenliği](#12-idempotency--tekrar-i̇şleme-güvenliği)
13. [Bilinen Hatalar](#13-bilinen-hatalar)
14. [Projeyi Çalıştırma](#14-projeyi-çalıştırma)

---

## 1. Mikroservis Nedir?

**Monolitik** bir uygulamada tüm özellikler (kullanıcı, ürün, sipariş, sepet) tek bir büyük JAR'da çalışır. Bir şey patlarsa her şey durur.

**Mikroservis** mimarisinde her iş alanı ayrı bir bağımsız uygulama olarak çalışır:

```
Monolitik              Mikroservis
┌───────────────┐      ┌──────────────┐  ┌──────────────┐
│               │      │ user-service │  │product-service│
│ user          │      │   :8081      │  │   :8082       │
│ product       │  →   └──────────────┘  └──────────────┘
│ order         │      ┌──────────────┐  ┌──────────────┐
│ cart          │      │ order-service│  │ cart-service  │
└───────────────┘      └──────────────┘  └──────────────┘
```

Her servis:
- Kendi veritabanına sahiptir (başkası doğrudan erişemez)
- Bağımsız deploy edilir
- Diğer servislerle HTTP veya Kafka (event) üzerinden haberleşir

---

## 2. Proje Genel Yapısı

```
microservices-spring/
│
├── docker/
│   └── docker-compose.yml        ← Kafka, PostgreSQL, pgAdmin başlatır
│
└── microservices/
    ├── pom.xml                   ← Parent (ana) Maven modülü
    │
    ├── eureka-server/            ← Servis kayıt defteri (port 8761)
    ├── gateway-server/           ← Tek giriş noktası (port 8888)
    ├── product-service/          ← Ürün işlemleri + Outbox (port 8082)
    ├── user-service/             ← Kullanıcı + Kafka consumer (port 8081)
    ├── cart-service/             ← Sepet (henüz iskelet)
    └── order-service/            ← Sipariş (henüz iskelet)
```

### Her servis kendi içinde şu yapıya sahiptir:

```
product-service/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/turkcell/product_service/
    │   │   ├── ProductServiceApplication.java   ← Başlangıç noktası
    │   │   ├── controller/                      ← HTTP endpoint'leri
    │   │   ├── entity/                          ← Veritabanı tabloları
    │   │   ├── repository/                      ← Veritabanı işlemleri
    │   │   ├── event/                           ← Kafka mesaj modelleri
    │   │   └── polling/                         ← Zamanlı görevler
    │   └── resources/
    │       └── application.yml                  ← Servis konfigürasyonu
    └── test/                                    ← Unit/integration testleri
```

---

## 3. pom.xml — Maven Yapısı

### 3.1 Parent pom.xml (microservices/pom.xml)

Bu dosya tüm servislerin **ortak ebeveyn** modülüdür. Bağımlılık versiyonlarını **bir yerden** yönetir.

```xml
<parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>4.0.6</version>       <!-- Spring Boot versiyonu burada değiştirilir -->
</parent>

<groupId>com.turkcell</groupId>
<artifactId>microservices</artifactId>
<version>0.0.1-SNAPSHOT</version>
<packaging>pom</packaging>          <!-- JAR değil, bu bir "konteyner" modül -->

<modules>
    <module>user-service</module>
    <module>product-service</module>
    <module>cart-service</module>
    <module>order-service</module>
    <module>eureka-server</module>
</modules>
```

**Neden parent pom?**
- Spring Cloud versiyonunu (`2025.1.1`) tüm servisler için tek yerden yönetmek için
- Her serviste tekrar yazmak zorunda kalmamak için ortak bağımlılıkları (`actuator`, `web`, `devtools`) burada tanımlamak için

### 3.2 Parent'taki ortak bağımlılıklar

```xml
<dependencies>
    <!-- HTTP endpoint yazabilmek için (Spring MVC) -->
    <dependency>spring-boot-starter-web</dependency>

    <!-- /actuator/health gibi endpoint'ler -- Eureka health check için gerekli -->
    <dependency>spring-boot-starter-actuator</dependency>

    <!-- Kodu değiştirince otomatik yeniden başlar (sadece geliştirme ortamı) -->
    <dependency>spring-boot-devtools</dependency>

    <!-- JUnit testleri -->
    <dependency>spring-boot-starter-test</dependency>
</dependencies>
```

### 3.3 product-service/pom.xml

```xml
<parent>
    <artifactId>microservices</artifactId>   <!-- Parent'a bağlanıyor -->
</parent>

<dependencies>
    <!-- Eureka'ya kayıt olabilmek için -->
    <dependency>spring-cloud-starter-netflix-eureka-client</dependency>

    <!-- Kafka ile mesaj gönderip alabilmek için (Spring Cloud Stream) -->
    <dependency>spring-cloud-starter-stream-kafka</dependency>

    <!-- JPA ile PostgreSQL'e kayıt yapabilmek için -->
    <dependency>spring-boot-starter-data-jpa</dependency>

    <!-- PostgreSQL JDBC sürücüsü (runtime: sadece uygulama çalışırken lazım) -->
    <dependency>
        <artifactId>postgresql</artifactId>
        <scope>runtime</scope>
    </dependency>
</dependencies>
```

**Neden `scope: runtime`?**
Derleme sırasında PostgreSQL sürücüsüne ihtiyaç yok; uygulama ayağa kalkarken bağlantı kurulduğunda gerekli. `runtime` scope bunu sağlar.

### 3.4 eureka-server/pom.xml

```xml
<!-- Sadece şu fark var: client değil SERVER bağımlılığı -->
<dependency>spring-cloud-starter-netflix-eureka-server</dependency>
```

### 3.5 gateway-server/pom.xml

```xml
<!-- Reactive Gateway (Spring MVC tabanlı yeni versiyon) -->
<dependency>spring-cloud-starter-gateway-server-webmvc</dependency>

<!-- Gateway aynı zamanda Eureka client'ı — lb:// prefix ile yönlendirme yapabilsin -->
<dependency>spring-cloud-starter-netflix-eureka-client</dependency>
```

---

## 4. Her Servisin Katmanları

Katmanlı mimari, sorumluluğu böler. Her dosyanın tek bir görevi vardır.

### 4.1 `*Application.java` — Başlangıç Noktası

```java
@SpringBootApplication      // @Configuration + @ComponentScan + @EnableAutoConfiguration
@EnableScheduling           // @Scheduled anotasyonlarını aktif eder (OutboxPoller için)
public class ProductServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(ProductServiceApplication.class, args);
    }
}
```

Bu dosya olmadan Spring context başlamaz. Her servisin sadece bir tane bu dosyası olur.

### 4.2 `entity/` — Veritabanı Tabloları

```java
@Entity
@Table(name = "outbox")
public class OutboxEvent {

    @Id
    private UUID id;                    // PRIMARY KEY

    private String aggregateType;       // Hangi domain? ("Product")
    private String aggregateId;         // Hangi nesnenin ID'si?
    private String eventType;           // Hangi olay? ("testEvent")

    @Column(columnDefinition = "TEXT")
    private String payload;             // JSON olarak event içeriği

    @Enumerated(EnumType.STRING)        // DB'ye "PENDING" string olarak yazar
    private OutboxStatus status;        // PENDING / SENT / FAILED

    private int retryCount;
    private Instant createdAt;
    private Instant processedAt;
    private String errorMessage;
}
```

**Neden `@Column(columnDefinition = "TEXT")`?**  
`String` varsayılan olarak `VARCHAR(255)` olur. Büyük JSON payload için `TEXT` gerekir.

**Neden UUID?**  
`Long` ID'ler sıralı ve tahmin edilebilirdir. UUID hem güvenli hem de dağıtık sistemlerde çakışma olmaz.

### 4.3 `repository/` — Veritabanı Erişim Katmanı

```java
public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    // Spring Data JPA bu metodu otomatik SQL'e çevirir:
    // SELECT * FROM outbox WHERE status = 'PENDING' ORDER BY created_at LIMIT :limit
    @Query("SELECT o FROM OutboxEvent o WHERE o.status = 'PENDING' ORDER BY o.createdAt ASC")
    List<OutboxEvent> findPublishable(int limit);
}
```

`JpaRepository`'yi extend edince şunlar **bedavaya** gelir:
- `save(entity)` — kaydet/güncelle
- `findById(id)` — ID ile getir
- `findAll()` — hepsini getir
- `deleteById(id)` — sil
- `count()` — say

### 4.4 `controller/` — HTTP Endpoint Katmanı

```java
@RestController                   // @Controller + @ResponseBody
@RequestMapping("/api/products")  // Tüm endpoint'ler bu prefix ile başlar
public class ProductsController {

    @GetMapping
    public ResponseEntity<String> createProduct(@RequestParam String message) {
        // İş mantığı burada — event oluştur, outbox'a kaydet
        return ResponseEntity.ok("Event kaydedildi");
    }
}
```

**Katman kuralı:** Controller sadece HTTP isteğini alır ve iş mantığını Service katmanına devreder. Bu projede henüz ayrı bir `service/` katmanı yok; küçük projeler için controller'da doğrudan yapılıyor.

### 4.5 `event/` — Kafka Mesaj Modelleri

```java
// Record = immutable (değiştirilemez) veri sınıfı, Java 16+
public record TestEvent(UUID eventId, String message, UUID productId) {}
```

**Neden `record`?**
- Otomatik `equals`, `hashCode`, `toString` sağlar
- `final` alanlar — immutable
- Kafka mesajları değişmemeli, record bu için biçilmiş kaftan

### 4.6 `polling/` — Zamanlı Görevler

```java
@Component
@Scheduled(fixedDelay = 20000)    // Her 20 saniyede bir çalışır
public void publishPendingEvents() {
    // DB'den PENDING event'leri çek → Kafka'ya gönder → status güncelle
}
```

---

## 5. Enum Kullanımı

```java
public enum OutboxStatus {
    PENDING,     // DB'ye yazıldı, henüz Kafka'ya gönderilmedi
    SENT,        // Kafka'ya başarıyla gönderildi   ← ⚠️ ŞUANDA EKSİK! (bkz. §13)
    FAILED       // 3 deneme sonrası başarısız
}
```

**Neden enum?**

| Yaklaşım | Sorun |
|---|---|
| `String status = "pending"` | Yazım hatası olabilir: `"panding"` → runtime'da patlıyor |
| `int status = 1` | `1` ne anlama geliyor? Kod okunaksız |
| `enum OutboxStatus` | Derleme hatası verir, IDE tamamlar, okunabilir |

**Entity'de enum kullanımı:**

```java
@Enumerated(EnumType.STRING)    // DB'ye "PENDING" string yazar (okunabilir)
// @Enumerated(EnumType.ORDINAL) // DB'ye 0/1/2 yazar (KÖTÜ — sıra değişince data bozulur)
private OutboxStatus status;
```

**Dikkat:** `EnumType.ORDINAL` kullanmayın. Eğer enum'a yeni değer eklerseniz, eski kayıtların anlamı değişir.

---

## 6. application.yml — Konfigürasyon

### product-service/src/main/resources/application.yml

```yaml
spring:
  application:
    name: product-service      # Eureka'daki kayıt adı; lb://product-service ile çağrılır

  datasource:
    url: jdbc:postgresql://localhost:5433/products   # docker-compose'daki product-db
    username: postgres
    password: test12345

  jpa:
    hibernate:
      ddl-auto: update         # Uygulama başlarken tabloları otomatik oluştur/güncelle
                               # prod'da "validate" veya "none" kullanılır

  cloud:
    stream:
      bindings:
        testEvent-out-0:       # "testEvent" fonksiyon adı + "-out-" + "0" index
          destination: test-topic   # Kafka topic adı
      kafka:
        binder:
          brokers: localhost:9092

server:
  port: 8082
```

### user-service/src/main/resources/application.yml

```yaml
  cloud:
    stream:
      bindings:
        consumeTestEvent-in-0:      # "consumeTestEvent" Bean adı + "-in-" + "0"
          destination: test-topic   # Aynı topic'i dinliyor
          group: user-service-group # Consumer group — partition'lar bu group'a dağıtılır
```

**`ddl-auto` seçenekleri:**

| Değer | Ne Yapar | Ne Zaman |
|---|---|---|
| `create` | Tabloları baştan oluşturur (mevcut data silinir) | Sadece ilk geliştirme |
| `update` | Eksik sütun/tablo ekler, mevcutu değiştirmez | Geliştirme ortamı |
| `validate` | Tablo yapısını kontrol eder, değiştirmez | Test/staging |
| `none` | Hiçbir şey yapmaz | Production |

---

## 7. docker-compose.yml — Altyapı

Docker Compose, tüm altyapı bileşenlerini (Kafka, DB) tek komutla başlatır.

```yaml
services:

  kafka:
    image: apache/kafka:4.2.0
    ports:
      - "9092:9092"          # HOST:CONTAINER — localhost:9092 → container içi 9092
    environment:
      KAFKA_PROCESS_ROLES: broker,controller    # KRaft: ZooKeeper'a gerek yok
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"   # Eğitim için: topic otomatik oluşur
      KAFKA_NUM_PARTITIONS: 3                   # Her otomatik topic 3 partition'lı

  kafka-ui:
    image: kafbat/kafka-ui:latest
    ports:
      - "8080:8080"          # http://localhost:8080 — Kafka'yı görsel yönet

  product-db:
    image: postgres:17
    environment:
      POSTGRES_DB: products   # Veritabanı adı
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: test12345
    ports:
      - "5433:5432"           # 5432 zaten kullanımdaysa dışarıya 5433'ten açıyoruz

  user-db:
    image: postgres:17
    environment:
      POSTGRES_DB: users
    ports:
      - "5434:5432"           # product-db ile çakışmasın diye farklı host port

  pgadmin:
    image: dpage/pgadmin4:latest
    ports:
      - "5050:80"             # http://localhost:5050 — DB'leri görsel yönet
```

**Port Mantığı:**
```
HOST:CONTAINER
5433:5432  → "Dışarıdan 5433'e bağlan, container içinde 5432'ye ilet"
```
Her PostgreSQL container kendi içinde 5432'de dinler. Dışarıya farklı portlardan açarız ki çakışmasın.

---

## 8. Eureka — Servis Keşfi

### Problem

Mikroservisler birbirini nasıl bulur? IP adresi sabit değil, servisler ölüp yeniden başlıyor.

### Çözüm: Eureka

```
┌─────────────────────────┐
│     eureka-server        │ ← DNS gibi — "user-service nerede?"
│  http://localhost:8761   │
└─────────┬───────────────┘
          │ kayıt
    ┌─────┴──────┐
    │            │
product-service  user-service
"Ben :8082'deyim" "Ben :8081'deyim"
```

### eureka-server/application.yml

```yaml
server:
  port: 8761

eureka:
  instance:
    hostname: localhost
  client:
    register-with-eureka: false   # Kendini kendine kaydetme
    fetch-registry: false         # Başka servis listesi çekme
```

### Servis tarafında (product-service/application.yml)

```yaml
eureka:
  client:
    service-url:
      defaultZone: http://localhost:8761/eureka/
```

`@EnableEurekaServer` anotasyonu EurekaServerApplication'da yeterli. İstemciler için sadece `application.yml` konfigürasyonu yeterlidir — bağımlılık zaten `eureka-client` starter sayesinde otomatik etkinleşir.

---

## 9. Gateway — API Geçidi

### Problem

Dışarıdan her servise ayrı port ile bağlanmak zorunda mıyız?

```
Kötü:
client → localhost:8081 (user)
client → localhost:8082 (product)
client → localhost:8083 (cart)
```

### Çözüm: Gateway

```
İyi:
client → localhost:8888 (gateway)
                ↓
         /api/users/** → user-service
         /api/products/** → product-service
```

### gateway-server/application.yml

```yaml
spring:
  cloud:
    gateway:
      mvc:
        routes:
          - id: user-service
            uri: lb://user-service        # lb = Load Balancer (Eureka'dan IP alır)
            predicates:
              - Path=/api/users/**        # Bu path'e gelen istekler user-service'e git
```

`lb://user-service` — Gateway, Eureka'ya sorar: "user-service nerede?" ve oraya iletir. Doğrudan IP yazmak gerekmez.

---

## 10. Kafka — Olay Mesajlaşması

### Spring Cloud Stream ile Kafka

Kafka'yı doğrudan kullanmak yerine **Spring Cloud Stream** soyutlaması kullanılıyor. Böylece Kafka'yı RabbitMQ ile değiştirsek de uygulama kodu değişmez.

### Mesaj Gönderme (product-service)

```java
// StreamBridge: istediğin zaman, istediğin yerden mesaj gönder
@Autowired
private StreamBridge streamBridge;

streamBridge.send("testEvent-out-0", payload);
//                    ↑ binding adı (application.yml'daki anahtar)
```

`application.yml`'da `testEvent-out-0` → `test-topic` olarak eşlenmiş.

### Mesaj Alma (user-service)

```java
@Bean
public Consumer<TestEvent> consumeTestEvent() {
    return event -> {
        // Kafka'dan gelen her mesajda bu lambda çalışır
        System.out.println("Mesaj geldi: " + event.message());
    };
}
```

Bean adı `consumeTestEvent` → `application.yml`'daki `consumeTestEvent-in-0` binding'iyle eşleşir.

### Consumer Group

```yaml
group: user-service-group
```

Aynı group'tan birden fazla user-service instance'ı çalışıyorsa, her mesaj **sadece bir** instance tarafından işlenir (yük dağılımı). Farklı group'lar aynı mesajı her biri alır.

---

## 11. Outbox Pattern — Güvenilir Yayın

### Problem

```java
// YANLIŞ: İki işlem atomik değil!
repository.save(product);      // ← bu başarılı
kafkaTemplate.send(event);     // ← bu patlıyorsa? Event kaybolur!
```

### Çözüm: Outbox Pattern

```
1. HTTP İsteği Gelir
        ↓
2. Aynı DB Transaction'ında:
   - products tablosuna kayıt
   - outbox tablosuna PENDING event
        ↓
3. OutboxPoller (her 20sn):
   - DB'den PENDING event'leri oku
   - Kafka'ya gönder
   - Status'u SENT yap
```

```java
// ProductsController.java
OutboxEvent event = new OutboxEvent();
event.setId(UUID.randomUUID());
event.setAggregateType("Product");
event.setEventType("testEvent");
event.setPayload(json);
event.setStatus(OutboxStatus.PENDING);   // Başlangıçta PENDING
outboxRepository.save(event);            // DB'ye yaz — Kafka henüz yok

// OutboxPoller.java (ayrı thread, 20sn'de bir)
List<OutboxEvent> events = outboxRepository.findPublishable(100);
for (OutboxEvent e : events) {
    streamBridge.send(e.getEventType() + "-out-0", e.getPayload());
    e.setStatus(OutboxStatus.SENT);     // Başarılı → SENT
    // Hata varsa: retryCount++ → 3'e ulaşınca FAILED
}
```

**Neden güvenilir?** DB ve Kafka aynı anda commit edilmiyor. DB transaction başarılıysa event **mutlaka** outbox'ta. Poller onu **mutlaka** Kafka'ya gönderir.

---

## 12. Idempotency — Tekrar İşleme Güvenliği

### Problem

Kafka "at-least-once" garantisi verir — aynı mesaj **birden fazla** gelebilir.

```
Kafka → user-service: "eventId: ABC123, mesaj: ..."
Kafka → user-service: "eventId: ABC123, mesaj: ..."  ← aynı mesaj tekrar!
```

### Çözüm: ProcessedEvent Tablosu

```java
// TestEventConsumer.java
@Bean
public Consumer<TestEvent> consumeTestEvent() {
    return event -> {
        // Daha önce işledik mi?
        if (processedEventRepository.existsById(event.eventId())) {
            return;  // Evet → atla
        }

        // Hayır → işle ve kaydet
        processedEventRepository.save(
            new ProcessedEvent(event.eventId(), Instant.now())
        );
    };
}
```

`ProcessedEvent` tablosu bir kayıt defteri: "Bu event'i işledim."

---

## 13. Bilinen Hatalar

Proje henüz geliştirme aşamasında, iki kritik hata var:

### Hata 1: `OutboxStatus.SENT` Enum'da Eksik

`OutboxPoller.java:37` satırında `OutboxStatus.SENT` kullanılıyor ama `OutboxStatus` enum'unda sadece `PENDING`, `PROCESSED`, `FAILED` var.

**Düzeltme** (`entity/OutboxStatus.java`):
```java
public enum OutboxStatus {
    PENDING,
    SENT,      // ← Bu satırı ekle
    FAILED
}
```

### Hata 2: `OutboxRepository.java` Bozuk

`product-service/repository/OutboxRepository.java` dosyası yanlış içerik barındırıyor (ProductServiceApplication sınıfını içeriyor). Dosyanın içeriği şöyle olmalı:

```java
package com.turkcell.product_service.repository;

import com.turkcell.product_service.entity.OutboxEvent;
import com.turkcell.product_service.entity.OutboxStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface OutboxRepository extends JpaRepository<OutboxEvent, UUID> {

    @Query("SELECT o FROM OutboxEvent o WHERE o.status = :status ORDER BY o.createdAt ASC LIMIT :limit")
    List<OutboxEvent> findPublishable(@Param("limit") int limit,
                                     @Param("status") OutboxStatus status);
}
```

Ve `OutboxPoller.java`'da çağrı:
```java
outboxRepository.findPublishable(100, OutboxStatus.PENDING);
```

### Hata 3: Paket Adı Yazım Hatası

`user-service`'te `entiity/` (çift i) paketi var, `entity/` olmalı.

---

## 14. Projeyi Çalıştırma

### Adım 1: Altyapıyı Başlat (Docker)

```powershell
# docker/ klasörüne git
cd d:\microservices-spring\docker

# Kafka, PostgreSQL, pgAdmin'i arka planda başlat
docker compose up -d

# Servislerin ayakta olduğunu kontrol et
docker compose ps

# Kafka loglarını izle (hazır olması 10-20sn sürebilir)
docker compose logs -f kafka
```

**Hazır olduğunu nasıl anlarsın?**
`kafka-ui` aç: http://localhost:8080 — Cluster görünüyorsa Kafka hazır.

### Adım 2: Eureka Server'ı Başlat

```powershell
# microservices/ ana klasöründe
cd d:\microservices-spring\microservices

# Sadece eureka-server'ı derle ve çalıştır
mvn spring-boot:run -pl eureka-server
```

Tarayıcıda http://localhost:8761 açılırsa Eureka hazır.

### Adım 3: Servisleri Başlat

Her biri için ayrı terminal aç:

**Terminal 1 — product-service:**
```powershell
cd d:\microservices-spring\microservices
mvn spring-boot:run -pl product-service
```

**Terminal 2 — user-service:**
```powershell
cd d:\microservices-spring\microservices
mvn spring-boot:run -pl user-service
```

**Terminal 3 — gateway-server:**
```powershell
cd d:\microservices-spring\microservices
mvn spring-boot:run -pl gateway-server
```

### Adım 4: Test Et

```powershell
# product-service'e doğrudan istek (port 8082)
curl "http://localhost:8082/api/products?message=MerhabaKafka"

# gateway üzerinden istek (port 8888) — prod ortamı bu şekilde olur
curl "http://localhost:8888/api/products?message=MerhabaGateway"
```

### Adım 5: Durumu İzle

| URL | Ne Gösterir |
|---|---|
| http://localhost:8761 | Eureka — hangi servisler kayıtlı |
| http://localhost:8080 | Kafka UI — topic'ler, mesajlar |
| http://localhost:5050 | pgAdmin — DB tabloları (admin@admin.com / admin) |

### Tüm Projeyi Derle (çalıştırmadan)

```powershell
cd d:\microservices-spring\microservices
mvn clean package -DskipTests
```

### Altyapıyı Durdur

```powershell
cd d:\microservices-spring\docker
docker compose down           # Container'ları durdur, volume'lar kalır
docker compose down -v        # Volume'larla birlikte sil (data gider!)
```

### Başlatma Sırası Özeti

```
1. docker compose up -d          ← Altyapı (Kafka, DB)
2. eureka-server                 ← Servis kayıt defteri
3. gateway-server                ← (isteğe bağlı, son başlat)
4. product-service               ← Herhangi bir sırayla
5. user-service                  ← Herhangi bir sırayla
```

Eureka olmadan başlatılan servisler bağlanamayıp hata verir ama yeniden denemeye devam eder. Eureka hazır olunca otomatik kaydolurlar.

---

## Mimari Özet

```
[Client]
    │
    ▼
[Gateway :8888]  ─── Eureka'dan IP alır
    │
    ├── /api/products/** ──► [product-service :8082]
    │                              │
    │                         PostgreSQL (products DB :5433)
    │                              │
    │                         OutboxPoller ──► Kafka (test-topic)
    │                                               │
    └── /api/users/**  ──► [user-service :8081] ◄──┘
                                   │
                              PostgreSQL (users DB :5434)

Tüm servisler Eureka'ya (:8761) kayıtlıdır.
```
