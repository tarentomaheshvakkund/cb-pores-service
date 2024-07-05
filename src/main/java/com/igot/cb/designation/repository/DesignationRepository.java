package com.igot.cb.designation.repository;

import com.igot.cb.designation.entity.DesignationEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DesignationRepository extends JpaRepository<DesignationEntity, String> {

}
