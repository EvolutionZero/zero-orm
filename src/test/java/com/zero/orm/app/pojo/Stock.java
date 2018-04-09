package com.zero.orm.app.pojo;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import com.zero.orm.core.PojoBaseBean;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@Table(name="t_code")
public class Stock extends PojoBaseBean{

	@Getter
	@Setter
	@Id
	@Column(name="code")
	private String code;
	
	@Getter
	@Setter
	@Column(name="isStock")
	private String isStock;
	
	@Getter
	@Setter
	@Column(name="name")
	private String name;
	
	@Getter
	@Setter
	@Column(name="status")
	private Integer status;
	
	@Getter
	@Setter
	@Column(name="industry")
	private String industry;
	
	@Getter
	@Setter
	@Column(name="listDate")
	private Date listDate;
	
	@Getter
	@Setter
	@Column(name="location")
	private String location;
	
	@Getter
	@Setter
	@Column(name="bussiness")
	private String bussiness;
	
}
