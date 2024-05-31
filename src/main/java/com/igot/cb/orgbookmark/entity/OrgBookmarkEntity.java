package com.igot.cb.orgbookmark.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Timestamp;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "orgBookmark")
@TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)
@Entity
public class OrgBookmarkEntity {
    @Id
    private String orgBookmarkId;

    @Type(type = "jsonb")
    @Column(columnDefinition = "jsonb")
    private JsonNode data;

    private Timestamp createdOn;

    private Timestamp updatedOn;
}
