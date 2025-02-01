package com.app.core;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;


@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProductCreatedEvent
{
	private String productId;
	private String title;
	private BigDecimal price;
	private Integer quantity;
}
