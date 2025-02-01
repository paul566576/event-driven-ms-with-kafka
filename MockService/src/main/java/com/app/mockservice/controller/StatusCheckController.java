package com.app.mockservice.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/response")
public class StatusCheckController
{

	@GetMapping("/200")
	public ResponseEntity<String> getOK()
	{
		return ResponseEntity.ok().body("200");
	}

	@GetMapping("/500")
	public ResponseEntity<String> getServerInternalError() {
		return ResponseEntity.internalServerError().body("500");
	}
}
