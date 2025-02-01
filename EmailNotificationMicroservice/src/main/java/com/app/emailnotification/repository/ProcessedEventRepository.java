package com.app.emailnotification.repository;

import com.app.emailnotification.io.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;


@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long>
{
	Optional<ProcessedEventEntity> findByMessageId(final String messageId);
}
