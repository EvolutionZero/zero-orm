package com.zero.orm.app.pojo;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import com.zero.orm.core.PojoBaseBean;

@Table(name="T_SDN_PW_PATH")
public class Shape extends PojoBaseBean{

	public Shape() {
		
	}
	
	public Shape(HashMap<String, Object> resultSet) {
		super(resultSet);
	}

	public static void main(String[] args) {
		HashMap<String, Object> resultSet = new HashMap<String, Object>();
		resultSet.put("A_PW_ID", new Long(1024));
		resultSet.put("PATH_TYPE", new Integer(1));
		resultSet.put("A_PORT_KEY", "ABCS");
		resultSet.put("PW_PATH_KEY", "xyz");
		resultSet.put("IS_ALL", new Boolean(true));
		resultSet.put("ALL", new Boolean(true));
		resultSet.put("B_ALL", true);
		resultSet.put("L_X", 2048);
		resultSet.put("WIDTH", new BigDecimal(5566));
		resultSet.put("INSERT_TIME", new Timestamp(new Date().getTime()));
		
		Shape pwPath = new Shape(resultSet);
		System.out.println("生成：" + pwPath);
		System.out.println("参数化：" + pwPath.getParamList());
	}
	
	@Id
	@Column(name="PW_PATH_KEY")
	private String pwPathKey;
	
	@Column(name="ALL")
	private Boolean all;
	
	@Column(name="IS_ALL")
	private Boolean isAll;
	
	@Column(name="B_ALL")
	private boolean bAll;
	
	@Column(name="PW_KEY")
	private String pwKey;
	
	
	@Column(name="L_X")
	private long lx;
	
	@Column(name="PATH_TYPE")
	private Integer pathType;
	
	@Column(name="INSERT_TIME", updatable = false)
	private Timestamp insertTime;
	
	@Column(name="WIDTH")
	private BigDecimal width;
	
	@Column(name="WORK_STATUS")
	private Integer workStatus;
	
	@Column(name="A_PORT_KEY")
	private String aPortKey;

	public String getPwPathKey() {
		return pwPathKey;
	}

	public Boolean getAll() {
		return all;
	}

	public Boolean getIsAll() {
		return isAll;
	}

	public boolean isbAll() {
		return bAll;
	}

	public String getPwKey() {
		return pwKey;
	}

	public long getLx() {
		return lx;
	}

	public Integer getPathType() {
		return pathType;
	}

	public Timestamp getInsertTime() {
		return insertTime;
	}

	public BigDecimal getWidth() {
		return width;
	}

	public Integer getWorkStatus() {
		return workStatus;
	}

	public String getaPortKey() {
		return aPortKey;
	}

	public void setPwPathKey(String pwPathKey) {
		this.pwPathKey = pwPathKey;
	}

	public void setAll(Boolean all) {
		this.all = all;
	}

	public void setIsAll(Boolean isAll) {
		this.isAll = isAll;
	}

	public void setbAll(boolean bAll) {
		this.bAll = bAll;
	}

	public void setPwKey(String pwKey) {
		this.pwKey = pwKey;
	}

	public void setLx(long lx) {
		this.lx = lx;
	}

	public void setPathType(Integer pathType) {
		this.pathType = pathType;
	}

	public void setInsertTime(Timestamp insertTime) {
		this.insertTime = insertTime;
	}

	public void setWidth(BigDecimal width) {
		this.width = width;
	}

	public void setWorkStatus(Integer workStatus) {
		this.workStatus = workStatus;
	}

	public void setaPortKey(String aPortKey) {
		this.aPortKey = aPortKey;
	}

	@Override
	public String toString() {
		return "Shape [pwPathKey=" + pwPathKey + ", all=" + all + ", isAll="
				+ isAll + ", bAll=" + bAll + ", pwKey=" + pwKey + ", lx=" + lx
				+ ", pathType=" + pathType + ", insertTime=" + insertTime
				+ ", width=" + width + ", workStatus=" + workStatus
				+ ", aPortKey=" + aPortKey + "]";
	}

	
}
