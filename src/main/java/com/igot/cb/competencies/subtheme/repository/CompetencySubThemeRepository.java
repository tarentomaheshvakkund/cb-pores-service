package com.igot.cb.competencies.subtheme.repository;

import com.igot.cb.competencies.subtheme.entity.CompetencySubThemeEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface CompetencySubThemeRepository extends JpaRepository<CompetencySubThemeEntity, String> {

}
