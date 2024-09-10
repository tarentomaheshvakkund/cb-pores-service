package com.igot.cb.contentpartner.repository;

import com.igot.cb.contentpartner.entity.ContentPartnerEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ContentPartnerRepository extends JpaRepository<ContentPartnerEntity, String>{
    Optional<ContentPartnerEntity> findByIdAndIsActive(String exitingId,Boolean isActive);

    @Query(value = "SELECT * FROM content_partner WHERE data->>'orgId' = :orgId", nativeQuery = true)
    Optional<ContentPartnerEntity> findByContentOrgId(String orgId);

    @Query(value = "SELECT * FROM content_partner WHERE data->>'contentPartnerName' = :partnerName AND data->>'orgId' = :orgId", nativeQuery = true)
    Optional<ContentPartnerEntity> findByContentPartnerNameAndOrgId(@Param("partnerName") String partnerName, @Param("orgId") String orgId);
}