package com.app.emailnotification.event.handler;

import com.app.core.ProductCreatedEvent;
import com.app.emailnotification.exception.NotRetryableException;
import com.app.emailnotification.exception.RetryableException;
import com.app.emailnotification.io.ProcessedEventEntity;
import com.app.emailnotification.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;


@Component
@KafkaListener(topics = "product-created-event-topic")
@RequiredArgsConstructor
@Slf4j
public class ProductCreatedEventHandler
{
	private final RestTemplate restTemplate;
	private final ProcessedEventRepository processedEventRepository;

	@KafkaHandler
	@Transactional
	public void handle(@Payload final ProductCreatedEvent productCreatedEvent,
			@Header(value = "messageId", required = true) final String messageId,
			@Header(KafkaHeaders.RECEIVED_KEY) final String messageKey)
	{
		//		if (true)
		//			throw new RetryableException("Something went wrong. We don't need to proceed with this event");

		log.info("Handled product created event: " + productCreatedEvent.getTitle());
		log.info("Handled product created event: " + productCreatedEvent.getProductId());


		// Check if the message as been already processed before
		final Optional<ProcessedEventEntity> processedEventEntity = processedEventRepository.findByMessageId(
				messageId);
		if (processedEventEntity.isPresent())
		{
			log.info("ProcessedEventEntity with messageId " + messageId + " already exists");
			return;
		}
		try
		{
			final ResponseEntity<String> responseEntity = restTemplate.exchange("http://localhost:8082/response/200",
					HttpMethod.GET,
					null,
					String.class);
			if (responseEntity.getStatusCode().value() == HttpStatus.OK.value())
			{
				log.info("Receive response from Remote service: " + responseEntity.getBody());
			}
		}
		catch (final ResourceAccessException e)
		{
			log.error(e.getMessage());
			throw new RetryableException(e);
		}
		catch (final HttpServerErrorException e)
		{
			log.error(e.getMessage());
			throw new NotRetryableException(e);
		}
		catch (final Exception e)
		{
			log.error(e.getMessage());
			throw new NotRetryableException(e);
		}

		//save uniq message in in DB table
		try
		{
			final ProcessedEventEntity newProcessedEventEntity = processedEventRepository.save(
					new ProcessedEventEntity(messageId, productCreatedEvent.getProductId()));

			log.info("ProcessedEventEntity has been saved with id: " + newProcessedEventEntity.getId());
		}
		catch (final DataIntegrityViolationException e)
		{
			log.error(e.getMessage());
			throw new NotRetryableException(e);
		}
	}
}
