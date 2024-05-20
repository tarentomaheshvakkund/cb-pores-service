package com.igot.cb.demand.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import java.sql.Timestamp;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "demands")
@TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)
@Entity
public class DemandEntity {
  @Id
  private String demandId;

  @Type(type = "jsonb")
  @Column(columnDefinition = "jsonb")
  private JsonNode data;

  private Timestamp createdOn;

  private Timestamp updatedOn;
}
