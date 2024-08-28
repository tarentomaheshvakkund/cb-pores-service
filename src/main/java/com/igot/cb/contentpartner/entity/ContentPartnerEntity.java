package com.igot.cb.contentpartner.entity;

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
@Table(name ="content_partner")
@TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)
@Entity
public class ContentPartnerEntity {
    @Id
    private String id;

    @Type(type = "jsonb")
    @Column(columnDefinition = "jsonb")
    private JsonNode data;

    private Timestamp createdOn;

    private Timestamp updatedOn;

    private Boolean isActive;

    @Type(type = "jsonb")
    @Column(columnDefinition = "jsonb")
    private JsonNode trasformContentJson;

    @Type(type = "jsonb")
    @Column(columnDefinition = "jsonb")
    private JsonNode transformProgressJson;

    @Type(type = "jsonb")
    @Column(columnDefinition = "jsonb")
    private JsonNode trasformCertificateJson;

    private Timestamp contentUploadLastUpdatedDate;

    private boolean isAuthenticate;

    private Timestamp contentProgressLastUpdatedDate;

    private String orgId;
}
