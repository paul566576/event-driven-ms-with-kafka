package com.app.emailnotification.exception;

public class NotRetryableException extends RuntimeException
{
	public NotRetryableException(Throwable cause)
	{
		super(cause);
	}

	public NotRetryableException(String message)
	{
		super(message);
	}
}
