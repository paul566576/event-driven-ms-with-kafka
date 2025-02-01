package com.app.product.productsmicroservice.controller;


import com.app.product.productsmicroservice.aspect.RestCounterAspect;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.atomic.AtomicLong;


@RestController
@RequestMapping("/products/counter")

public class CountController
{


	@GetMapping("/getCount")
	public ResponseEntity<AtomicLong> getCount() {
		return ResponseEntity.ok(RestCounterAspect.COUNTER);
	}
}
