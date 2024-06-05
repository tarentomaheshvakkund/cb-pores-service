package com.igot.cb.interest.repository;

import com.igot.cb.interest.entity.Interests;
import org.springframework.data.jpa.repository.JpaRepository;

public interface InterestRepository extends JpaRepository<Interests, String> {

}
