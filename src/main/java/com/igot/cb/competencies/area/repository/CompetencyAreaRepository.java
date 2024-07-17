package com.igot.cb.competencies.area.repository;

import com.igot.cb.competencies.area.entity.CompetencyAreaEntity;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CompetencyAreaRepository extends JpaRepository<CompetencyAreaEntity, String> {

  Optional<CompetencyAreaEntity> findByIdAndIsActive(String id, Boolean isActive);
}
