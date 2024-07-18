package com.igot.cb.competencies.theme.repository;

import com.igot.cb.competencies.theme.enity.CompetencyThemeEntity;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CompetencyThemeRepository extends JpaRepository<CompetencyThemeEntity, String> {

  Optional<CompetencyThemeEntity> findByIdAndIsActive(String id, boolean isActive);
}
