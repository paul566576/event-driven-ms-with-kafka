package com.app.emailnotification.io;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serial;
import java.io.Serializable;


@Entity
@Table(name = "processed-events")
@NoArgsConstructor
@Getter
@Setter
public class ProcessedEventEntity implements Serializable
{
	@Serial
	private static final long serialVersionUID = -8124578382862815958L;

	public ProcessedEventEntity(String messageId, String productId)
	{
		this.messageId = messageId;
		this.productId = productId;
	}

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(nullable = false, unique = true)
	private String messageId;

	@Column(nullable = false)
	private String productId;
}
