package com.app.product.productsmicroservice.service.impl;

import com.app.core.ProductCreatedEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


@SpringBootTest
public class IdempotentProducerIntegrationTest
{

	@Autowired
	private KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

	@MockBean
	private KafkaAdmin kafkaAdmin;

	@Test
	public void testProducerConfig_whenIdempotenceEnabled_assertsIdempotentProperties()
	{
		//Arrange
		final ProducerFactory<String, ProductCreatedEvent> producerFactory = kafkaTemplate.getProducerFactory();

		//Act
		final Map<String, Object> properties = producerFactory.getConfigurationProperties();

		//Asserts
		assertTrue((Boolean) properties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
		assertEquals("all", properties.get(ProducerConfig.ACKS_CONFIG));
		if (properties.containsKey(ProducerConfig.RETRIES_CONFIG))
		{
			assertTrue(
					Integer.parseInt(properties.get(ProducerConfig.RETRIES_CONFIG).toString()) > 0
			);
		}
	}
}
