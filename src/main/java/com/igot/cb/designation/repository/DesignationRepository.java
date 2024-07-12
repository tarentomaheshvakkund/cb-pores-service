package com.igot.cb.designation.repository;

import com.igot.cb.designation.entity.DesignationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface DesignationRepository extends JpaRepository<DesignationEntity, String> {
    Optional<DesignationEntity> findByIdAndIsActive(String id, Boolean isActive);
}
