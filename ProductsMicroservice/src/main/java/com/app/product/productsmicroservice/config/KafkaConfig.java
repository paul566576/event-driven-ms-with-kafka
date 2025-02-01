package com.app.product.productsmicroservice.config;

import com.app.core.ProductCreatedEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig
{

	public static final String PRODUCT_CREATED_EVENT_TOPIC = "product-created-event-topic";

	@Value("${spring.kafka.producer.bootstrap-servers}")
	private String bootstrapServers;
	@Value("${spring.kafka.producer.key-serializer}")
	private String keySerializer;
	@Value("${spring.kafka.producer.value-serializer}")
	private String valueSerializer;
	@Value("${spring.kafka.producer.acks}")
	private String acks;
	@Value("${spring.kafka.producer.properties.delivery.timeout.ms}")
	private String deliveryTimeout;
	@Value("${spring.kafka.producer.properties.linger.ms}")
	private String linger;
	@Value("${spring.kafka.producer.properties.request.timeout.ms}")
	private String requestTimeout;
	@Value("${spring.kafka.producer.properties.enable.idempotence}")
	private boolean idempotence;
	@Value("${spring.kafka.producer.properties.max.in.flight.requests.per.connection}")
	private Integer inflightsRequests;

	public Map<String, Object> producerConfig()
	{
		final Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
		props.put(ProducerConfig.ACKS_CONFIG, acks);
		props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeout);
		props.put(ProducerConfig.LINGER_MS_CONFIG, linger);
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeout);
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotence);
		props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, inflightsRequests);
		props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
		return props;
	}

	@Bean
	public ProducerFactory<String, ProductCreatedEvent> producerFactory()
	{
		return new DefaultKafkaProducerFactory<>(producerConfig());
	}

	@Bean
	public KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate()
	{
		return new KafkaTemplate<>(producerFactory());
	}

	@Bean
	public NewTopic createTopic()
	{
		return TopicBuilder.name(PRODUCT_CREATED_EVENT_TOPIC)
				.partitions(3)
				.replicas(3) // should be less or equal count of kafka brokers
				.configs(Map.of("min.insync.replicas", "2"))
				.build();
	}
}
