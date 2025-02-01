package com.app.product.productsmicroservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class CreateProductDTO
{
	private String title;
	private BigDecimal price;
	private Integer quantity;
}
