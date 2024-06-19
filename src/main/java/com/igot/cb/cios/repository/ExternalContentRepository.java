package com.igot.cb.cios.repository;

import com.igot.cb.cios.entity.ExternalContentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ExternalContentRepository extends JpaRepository<ExternalContentEntity, String> {

    Optional<ExternalContentEntity> findByExternalId(String externalId);

    List<ExternalContentEntity> findByIsActive(boolean b);
}
