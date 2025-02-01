package com.app.product.productsmicroservice.service.impl;

import com.app.core.ProductCreatedEvent;
import com.app.product.productsmicroservice.config.KafkaConfig;
import com.app.product.productsmicroservice.dto.CreateProductDTO;
import com.app.product.productsmicroservice.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;


@Service
@RequiredArgsConstructor
@Slf4j
public class ProductServiceImpl implements ProductService
{
	private final KafkaTemplate<String, ProductCreatedEvent> kafkaTemplate;

	@Override
	public String createProduct(CreateProductDTO productDTO) throws ExecutionException, InterruptedException
	{
		final String productId = UUID.randomUUID().toString();

		//TODO: persist Product Details into DB before publishing an Event

		final ProductCreatedEvent event = new ProductCreatedEvent(productId, productDTO.getTitle(), productDTO.getPrice(),
				productDTO.getQuantity());

		// this is async approach everything except join
		//		final CompletableFuture<SendResult<String, ProductCreatedEvent>> future = kafkaTemplate.send(
		//				KafkaConfig.PRODUCT_CREATED_EVENT_TOPIC, productId, event);
		//
		//		// In case when we need somehow react to message sending to kafka in async mode
		//		future.whenComplete((result, exception) -> {
		//			if (exception != null)
		//			{
		//				log.error("Failed to sent message: " + exception.getMessage());
		//			}
		//			else
		//			{
		//				log.info("Message sent successfully: " + result.getRecordMetadata());
		//			}
		//		});
		//
		//		// this will block current thread until confirmation will be received. Basically, this makes it synchronous operation
		//		future.join();


		log.info("Before publishing a Product event");
		//this is another sync way to sent event to kafka
		final ProducerRecord<String, ProductCreatedEvent> record = new ProducerRecord<>(KafkaConfig.PRODUCT_CREATED_EVENT_TOPIC,
				productId, event);
		record.headers().add("messageId", UUID.randomUUID().toString().getBytes());
		final SendResult<String, ProductCreatedEvent> result = kafkaTemplate.send(
				record).get();

		//some example how wee can debug publishing info to kafka
		log.info("Partition: " + result.getRecordMetadata().partition());
		log.info("Topic name: " + result.getRecordMetadata().topic());
		log.info("Timestamp: " + result.getRecordMetadata().timestamp());
		log.info("Offset: " + result.getRecordMetadata().offset());

		log.info("Returning product id: " + productId);
		return productId;
	}
}
