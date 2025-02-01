package com.app.product.productsmicroservice.service;

import com.app.product.productsmicroservice.dto.CreateProductDTO;

import java.util.concurrent.ExecutionException;


public interface ProductService
{
	String createProduct(final CreateProductDTO productDTO) throws ExecutionException, InterruptedException;
}
