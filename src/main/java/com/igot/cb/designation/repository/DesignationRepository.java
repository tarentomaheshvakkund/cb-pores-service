package com.igot.cb.designation.repository;

import com.igot.cb.designation.entity.DesignationEntity;
import com.igot.cb.interest.entity.Interests;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface DesignationRepository extends JpaRepository<DesignationEntity, String> {

  Optional<DesignationEntity> findByIdAndIsActive(String id, Boolean isActive);
}
