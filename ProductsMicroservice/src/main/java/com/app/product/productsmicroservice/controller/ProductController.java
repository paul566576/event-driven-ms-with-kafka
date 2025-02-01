package com.app.product.productsmicroservice.controller;

import com.app.product.productsmicroservice.dto.CreateProductDTO;
import com.app.product.productsmicroservice.dto.ErrorMessage;
import com.app.product.productsmicroservice.service.ProductService;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.concurrent.ExecutionException;


@RestController()
@RequestMapping("/products")
@AllArgsConstructor
@Slf4j
public class ProductController
{
	private final ProductService productService;

	@PostMapping
	public ResponseEntity<Object> createProduct(@RequestBody final CreateProductDTO product)
	{
		final String productId;
		try
		{
			productId = productService.createProduct(product);
		}
		catch (ExecutionException | InterruptedException e)
		{
			log.error(e.getCause().getMessage());
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
					.body(new ErrorMessage(new Date(), e.getMessage(), "/products"));
		}
		return ResponseEntity.status(HttpStatus.CREATED).body(productId);
	}


}
