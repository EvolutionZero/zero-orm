package com.zero.orm.app.pojo;

import java.sql.Date;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import com.zero.orm.core.PojoBaseBean;

@ToString
@Table(name="t_code", uniqueConstraints=@UniqueConstraint(columnNames={"name", "plate"}))
public class Stock extends PojoBaseBean{

	public Stock(){
		
	}
	
	public Stock(Map<String, Object> resultSet){
		super(resultSet);
	}
	
	@Getter
	@Setter
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Column(name="id")
	private Integer id;
	
	@Getter
	@Setter
	@Column(name="code")
	private String code;
	
	@Getter
	@Setter
	@Column(name="isStock")
	private String isStock;
	
	@Getter
	@Setter
	@Column(name="plate")
	private String plate;
	
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
