package com.app.product.productsmicroservice.aspect;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;


@Aspect
@Component
@Slf4j
public class RestCounterAspect
{
	public final static AtomicLong COUNTER = new AtomicLong();

	@Before("within(@org.springframework.web.bind.annotation.RestController *)")
	public void incrementCountBeforeAnyEndpointCall(final JoinPoint joinPoint) {
		final long current =  COUNTER.incrementAndGet();
		log.info("Current restController call number is: " + current);
		log.info("Calling method: {}.{}",
				joinPoint.getSignature().getDeclaringTypeName(),
				joinPoint.getSignature().getName());;
	}
}
