package com.igot.cb.cios.repository;



import com.igot.cb.cios.entity.CiosContentEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;


@Repository
public interface CiosRepository extends JpaRepository<CiosContentEntity,String> {
    Optional<CiosContentEntity> findByExternalIdAndPartnerId(String externalId,String PartnerId);

    Optional<CiosContentEntity> findByContentIdAndIsActive(String contentId, boolean b);
}
