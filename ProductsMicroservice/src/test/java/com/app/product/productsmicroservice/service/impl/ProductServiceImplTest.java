package com.app.product.productsmicroservice.service.impl;

import com.app.core.ProductCreatedEvent;
import com.app.product.productsmicroservice.dto.CreateProductDTO;
import com.app.product.productsmicroservice.service.ProductService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


@EmbeddedKafka(partitions = 3, count = 3, controlledShutdown = true)
@SpringBootTest(properties = "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}")
@ActiveProfiles("test") // application-test.yaml
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DirtiesContext
class ProductServiceImplTest
{
	@Autowired
	private ProductService productService;
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;
	@Autowired
	private Environment environment;

	private KafkaMessageListenerContainer<String, ProductCreatedEvent> container;
	private BlockingQueue<ConsumerRecord<String, ProductCreatedEvent>> record;

	@BeforeAll
	public void setUp()
	{
		final DefaultKafkaConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(
				getConsumerProperties());

		final ContainerProperties containerProperties = new ContainerProperties(
				environment.getProperty("product-created-events-topic-name", "product-created-event-topic"));
		container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
		record = new LinkedBlockingQueue<>();
		container.setupMessageListener((MessageListener<String, ProductCreatedEvent>) record::add);
		container.start();
		ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
	}

	@Test
	public void testCreatedProduct_whenGivenValidProductDetails_successfullySendsKafkaMessage()
			throws ExecutionException, InterruptedException
	{
		//Arrange //Given
		final String title = "iPhone 11";
		final BigDecimal price = new BigDecimal("600");
		final Integer quantity = 1;

		final CreateProductDTO product = new CreateProductDTO(title, price, quantity);

		//Act //When
		productService.createProduct(product);

		//Assert // Then
		final ConsumerRecord<String, ProductCreatedEvent> message = record.poll(3, TimeUnit.SECONDS);

		assertNotNull(message);
		assertNotNull(message.key());
		final ProductCreatedEvent productCreatedEvent =  message.value();
		assertEquals(productCreatedEvent.getQuantity(), product.getQuantity() );
		assertEquals(productCreatedEvent.getTitle(), product.getTitle() );
		assertEquals(productCreatedEvent.getPrice(), product.getPrice() );
	}


	private Map<String, Object> getConsumerProperties()
	{
		return Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString(),
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class,
				ConsumerConfig.GROUP_ID_CONFIG, environment.getProperty("spring.kafka.consumer.group-id", "default_group_id"),
				JsonDeserializer.TRUSTED_PACKAGES,
				environment.getProperty("*", "*"),
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				environment.getProperty("spring.kafka.consumer.auto-offset-reset", "earliest"));
	}

	@AfterAll
	public void tearDown()
	{
		container.stop();
	}

}
