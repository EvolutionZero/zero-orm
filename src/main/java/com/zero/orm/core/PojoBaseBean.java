package com.zero.orm.core;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PojoBaseBean {
	
	private static final Logger LOG = LoggerFactory.getLogger(PojoBaseBean.class);
	
	public PojoBaseBean(){
	}
	
	public PojoBaseBean(Map<String, Object> resultSet){
		Map<Field, Method> setterMap = ClassStructCache.FIELD_SETTER_MAPPING.get(getClass());
		Map<String, Field> columnNameFieldMap = ClassStructCache.getColumnNameFieldMap(getClass());
		for (Entry<String,Field> entry : columnNameFieldMap.entrySet()) {
			String columnName = entry.getKey();
			Field field = entry.getValue();
			
			Object value = resultSet.get(columnName) == null ? null : resultSet.get(columnName);
			Method setter = setterMap.get(field);
			try {
				setter.invoke(this, value);
			} catch (IllegalAccessException e) {
				LOG.error("", e);
			} catch (IllegalArgumentException e) {
				LOG.error("", e);
			} catch (InvocationTargetException e) {
				LOG.error("", e);
			}
		}
	}
	
	public String getQuerySql(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		
		StringBuilder sql = new StringBuilder("SELECT ");
		for (Column column : ClassStructCache.COLUMN_FIELD_MAPPING.get(getClass()).keySet()) {
			sql.append("`").append(column.name()).append("`").append(",");
		}
		if(sql.toString().endsWith(",")){
			sql.delete(sql.length() - 1, sql.length());
		}
		sql.append(" FROM ").append(tableName);
		return sql.toString();
		
	}
	
	public String getDeleteByIdSql(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		Map<Column, Field> columnFieldIdMap = ClassStructCache.COLUMN_FIELD_ID_MAPPING.get(getClass());
		
		StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableAnnotation.name()).append(" WHERE ");
		for (Entry<Column,Field> entry : columnFieldIdMap.entrySet()) {
			Column column = entry.getKey();
			
			sql.append("`").append(column.name()).append("`").append(" = ?").append(" AND ");
		}
		if(sql.toString().endsWith(" AND ")){
			sql.delete(sql.length() - " AND ".length(), sql.length());
		}
		return sql.toString();
		
	}
	
	public Object[] getDeleteByIdArray(){
		return getIdParmaArray();
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getDeleteByIdSql()
	 */
	
	public String getDeleteSql(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		
		StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName);
		return sql.toString();
		
	}
	
	public String getExistSql(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		Map<Column, Field> columnFieldIdMap = ClassStructCache.COLUMN_FIELD_ID_MAPPING.get(getClass());
		
		String tableName = tableAnnotation.name();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		for (Entry<Column,Field> entry : columnFieldIdMap.entrySet()) {
			Column column = entry.getKey();
			sql.append("`").append(column.name()).append("`").append(",");
		}
		if(sql.length() > 0){
			sql.delete(sql.length() - 1, sql.length());
		}
		sql.append(" FROM ").append(tableName).append(" WHERE ");
		for (Entry<Column,Field> entry : columnFieldIdMap.entrySet()) {
			Column column = entry.getKey();
			sql.append("`").append(column.name()).append("`").append(" = ? AND ");
		}
		if(sql.length() > 0){
			sql.delete(sql.length() - 4, sql.length());
		}
		return sql.toString();
		
	}
	
	public List<Object> getExistList(){
		List<Object> list = new LinkedList<Object>();
		Object[] paramArray = getExistArray();
		for (Object object : paramArray) {
			list.add(object);
		}
		return list;
	}
	
	public String getExistMD5(){
		StringBuilder sb = new StringBuilder();
		Object[] existArray = getExistArray();
		for (int i = 0; i < existArray.length; i++) {
			sb.append(existArray[i]).append("|");
		}
		return DigestUtils.md5Hex(sb.toString());
	}
	
	public Object[] getExistArray(){
		return getIdParmaArray();
		
	}

	private Object[] getIdParmaArray() {
		List<Field> columnField = new LinkedList<Field>();
		Map<Column, Field> columnFieldIdMap = ClassStructCache.COLUMN_FIELD_ID_MAPPING.get(getClass());
		
		for (Entry<Column,Field> entry : columnFieldIdMap.entrySet()) {
			columnField.add(entry.getValue());
		}
		Object[] params = new Object[columnField.size()];
		int idx = 0;
		for (Field field : columnField) {
			params[idx++] = getValue(field);
		}
		return params;
	}
	
	
	public List<Object> getParamList(){
		List<Object> list = new LinkedList<Object>();
		Object[] paramArray = getSaveArray();
		for (Object object : paramArray) {
			list.add(object);
		}
		return list;
	}
	
	public String getSaveSql(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(tableName).append("(");
		
		Map<Column, Field> columnFieldIdMap = ClassStructCache.COLUMN_FIELD_ID_MAPPING.get(getClass());
		Map<Column, Field> columnFieldMap = ClassStructCache.COLUMN_FIELD_MAPPING.get(getClass());
		
		StringBuilder values = new StringBuilder();
		for (Entry<Column,Field> entry : columnFieldIdMap.entrySet()) {
			Column column = entry.getKey();
			Field field = entry.getValue();
			
			if(isIdentity(field)){
				
			} else if(isSequence(field)){
				sql.append("`").append(column.name()).append("`").append(",");
				String sequenceName = getSequenceName(field);
				values.append(sequenceName).append(".NEXTVAL,");
				
			} else {
				sql.append("`").append(column.name()).append("`").append(",");
				values.append("?,");
			}
		}
		
		Collection<Field> idFields = columnFieldIdMap.values();
		for (Entry<Column,Field> entry : columnFieldMap.entrySet()) {
			Column column = entry.getKey();
			Field field = entry.getValue();
			
			if(!idFields.contains(field)){
				if(isUseDbTime(column)){
					sql.append("`").append(column.name()).append("`").append(",");
					values.append("#timestamp#,");
				} else {
					sql.append("`").append(column.name()).append("`").append(",");
					values.append("?,");
				}
			}
		}
		if(sql.toString().endsWith(",")){
			sql.delete(sql.length() - 1, sql.length());
			values.delete(values.length() - 1, values.length());
		}
		sql.append(") VALUES (").append(values).append(")");
		return sql.toString();
		
	}
	
	public Object[] getSaveArray(){
		List<Field> columnField = new LinkedList<Field>();
		Map<Column, Field> columnFieldIdMap = ClassStructCache.COLUMN_FIELD_ID_MAPPING.get(getClass());
		Map<Column, Field> columnFieldMap = ClassStructCache.COLUMN_FIELD_MAPPING.get(getClass());
		
		for (Entry<Column,Field> entry : columnFieldIdMap.entrySet()) {
			Field field = entry.getValue();
			
			if(isIdentity(field)){
				
			} else if(isSequence(field)){
				
			} else {
				columnField.add(field);
			}
		}
		
		Collection<Field> idFields = columnFieldIdMap.values();
		for (Entry<Column,Field> entry : columnFieldMap.entrySet()) {
			Column column = entry.getKey();
			Field field = entry.getValue();
			
			if(!idFields.contains(field)){
				if(isUseDbTime(column)){
				} else {
					columnField.add(field);
				}
			}
		}
		Object[] params = new Object[columnField.size()];
		int idx = 0;
		for (Field field : columnField) {
			params[idx++] = getValue(field);
		}
		return params;
	}
	
	public List<Object> getDeleteByIdList(){
		List<Object> list = new LinkedList<Object>();
		Object[] paramArray = getDeleteByIdArray();
		for (Object object : paramArray) {
			list.add(object);
		}
		return list;
	}
	
	
	private Object getValue(Field field){
		Map<Field, Method> getter = ClassStructCache.FIELD_GETTER_MAPPING.get(getClass());
		Object value = null;
		try {
			Method method = getter.get(field);
			value = method.invoke(this);
		} catch (IllegalAccessException e) {
			LOG.error("", e);
		} catch (IllegalArgumentException e) {
			LOG.error("", e);
		} catch (InvocationTargetException e) {
			LOG.error("", e);
		}
		return value;
	}
	
	private boolean isIdentity(Field field){
		boolean isIdentityStrategy = false;
		Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
		for (Annotation annotation : declaredAnnotations) {
			if(annotation instanceof GeneratedValue){
				GeneratedValue generatedValue = (GeneratedValue)annotation;
				if(generatedValue.strategy() == GenerationType.IDENTITY){
					isIdentityStrategy = true;
					break;
				}
			}
		}
		return isIdentityStrategy;
	}
	
	private boolean isSequence(Field field){
		boolean isSequenceStrategy = false;
		Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
		for (Annotation annotation : declaredAnnotations) {
			if(annotation instanceof GeneratedValue){
				GeneratedValue generatedValue = (GeneratedValue)annotation;
				if(generatedValue.strategy() == GenerationType.SEQUENCE){
					isSequenceStrategy = true;
					break;
				}
			}
		}
		return isSequenceStrategy;
	}
	
	private boolean isUseDbTime(Column column){
		boolean isUseDbTime = false;
		String columnDefinition = column.columnDefinition();
		if(columnDefinition.toUpperCase().contains("CURRENT_TIMESTAMP")
				|| columnDefinition.toUpperCase().contains("SYSDATE")){
			isUseDbTime = true;
		}
		return isUseDbTime;
	}
	
	private String getSequenceName(Field field){
		String sequenceName = "";
		Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
		for (Annotation annotation : declaredAnnotations) {
			if(annotation instanceof GeneratedValue){
				GeneratedValue generatedValue = (GeneratedValue)annotation;
				if(generatedValue.strategy() == GenerationType.SEQUENCE){
					String generator = generatedValue.generator();
					sequenceName = getSequenceName(generator, declaredAnnotations);
				}
			}
		}
		return sequenceName;
	}
	
	
	private String getSequenceName(String generator, Annotation[] declaredAnnotations){
		String sequenceName = "";
		for (Annotation annotation : declaredAnnotations) {
			if(annotation instanceof SequenceGenerator){
				SequenceGenerator sequenceGenerator = (SequenceGenerator)annotation;
				if(sequenceGenerator.name() != null && sequenceGenerator.name().equals(generator)){
					sequenceName = sequenceGenerator.sequenceName();
				}
			}
		}
		return sequenceName;
	}
	
}
