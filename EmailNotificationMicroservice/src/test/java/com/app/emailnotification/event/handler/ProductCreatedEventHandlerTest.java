package com.app.emailnotification.event.handler;

import com.app.core.ProductCreatedEvent;
import com.app.emailnotification.io.ProcessedEventEntity;
import com.app.emailnotification.repository.ProcessedEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.http.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;


@EmbeddedKafka
@SpringBootTest(properties = "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}")
public class ProductCreatedEventHandlerTest
{
	@MockBean
	private ProcessedEventRepository processedEventRepository;
	@MockBean
	private RestTemplate restTemplate;

	@Autowired
	private KafkaTemplate<String, Object> kafkaTemplate;

	@SpyBean
	private ProductCreatedEventHandler productCreatedEventHandler;

	@Test
	public void testProductCreatedEventHandler_onProductCreated_handlesEvent() throws ExecutionException, InterruptedException
	{
		//Arrange
		final ProductCreatedEvent event = new ProductCreatedEvent();
		event.setProductId(UUID.randomUUID().toString());
		event.setPrice(new BigDecimal("100.00"));
		event.setQuantity(1);
		event.setTitle("Test Product");

		final String messageId = UUID.randomUUID().toString();
		final String messageKey = event.getProductId();

		final ProducerRecord<String, Object> record = new ProducerRecord<>("product-created-event-topic", messageKey,
				event);
		record.headers().add("messageId", messageId.getBytes());
		record.headers().add(KafkaHeaders.RECEIVED_KEY, messageKey.getBytes());

		final ProcessedEventEntity processedEventEntity = new ProcessedEventEntity();
		when(processedEventRepository.save(any(ProcessedEventEntity.class))).thenReturn(null);
		when(processedEventRepository.findByMessageId(anyString())).thenReturn(Optional.of(processedEventEntity));

		final String responseBody = "{\"ket\":\"value\"}";
		final HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		final ResponseEntity<String> responseEntity = new ResponseEntity<>(responseBody, headers, HttpStatus.OK);
		when(restTemplate.exchange(
				anyString(),
				any(HttpMethod.class),
				isNull(),
				eq(String.class))).thenReturn(responseEntity);

		//Act
		kafkaTemplate.send(record).get();

		//Assert
		ArgumentCaptor<String> messageIdCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> messageKeyCaptor = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<ProductCreatedEvent> eventCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

		verify(productCreatedEventHandler, timeout(5000).times(1)).handle(eventCaptor.capture(),
				messageIdCaptor.capture(),
				messageKeyCaptor.capture());

		assertEquals(messageId, messageIdCaptor.getValue());
		assertEquals(messageKey, messageKeyCaptor.getValue());
		assertEquals(event.getProductId(), eventCaptor.getValue().getProductId());
	}
}
