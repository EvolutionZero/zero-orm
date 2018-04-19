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
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

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
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getQuerySql()
	 */
	
	public String getQuerySql(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		
		List<String> columnNames = new LinkedList<String>();
		Field[] declaredFields = this.getClass().getDeclaredFields();
		for (Field field : declaredFields) {
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Column){
					Column column = (Column)annotation;
					String columnName = column.name();
					columnNames.add(columnName);
				}
			}
		}
		StringBuilder sql = new StringBuilder("SELECT ");
		for (String columnName : columnNames) {
			sql.append(columnName).append(",");
		}
		if(sql.toString().endsWith(",")){
			sql.delete(sql.length() - 1, sql.length());
		}
		sql.append(" FROM ").append(tableName);
		return sql.toString();
		
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getSaveSql()
	 */
	
	
	public String getUpdateSql(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(tableName).append(" SET ");
		Field[] declaredFields = this.getClass().getDeclaredFields();
		for (Field field : declaredFields) {
			String columnName = "";
			boolean isId = false;
			boolean isUseDbTime = false;
			boolean isUpdatable = true;
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Id){
					isId = true;
				}
				if(annotation instanceof Column){
					Column column = (Column)annotation;
					isUpdatable = column.updatable();
					columnName = column.name();
					String columnDefinition = column.columnDefinition();
					if(columnDefinition.toUpperCase().contains("CURRENT_TIMESTAMP")
							|| columnDefinition.toUpperCase().contains("SYSDATE")){
						isUseDbTime = true;
					}
				}
			}
			if(!isId && isUpdatable){
				sql.append(columnName).append(" = ");
				if(isUseDbTime){
					sql.append("#timestamp#,");
				} else {
					sql.append("?,");
				}
			}
		}
		
		if(sql.toString().endsWith(",")){
			sql.delete(sql.length() - 1, sql.length());
		}
		return sql.toString();
		
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getUpdateSqlById()
	 */
	
	public String getUpdateSqlByUniqueConstraints(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		UniqueConstraint[] uniqueConstraints = tableAnnotation.uniqueConstraints();
		List<String> uniqueColumnNames = new LinkedList<String>();
		for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
			String[] columnNames = uniqueConstraint.columnNames();
			for (String columnName : columnNames) {
				uniqueColumnNames.add(columnName);
			}
		}
		if(uniqueColumnNames.isEmpty()){
			Field[] declaredFields = this.getClass().getDeclaredFields();
			for (Field field : declaredFields) {
				String columnName = "";
				boolean isId = false;
				Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
				for (Annotation annotation : declaredAnnotations) {
					if(annotation instanceof Id){
						isId = true;
					}
					if(annotation instanceof Column){
						Column column = (Column)annotation;
						columnName = column.name();
					}
				}
				if(isId && !"".equals(columnName)){
					uniqueColumnNames.add(columnName);
				}
			}
		}
		StringBuilder sql = new StringBuilder(getUpdateSql());
		sql.append(" WHERE ");
		for (String columnName : uniqueColumnNames) {
			sql.append(columnName).append(" = ? AND ");
		}
		if(sql.length() > 0){
			sql.delete(sql.length() - 4, sql.length());
		}
		return sql.toString();
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getDeleteSql()
	 */
	
	public String getDeleteByIdSql(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		List<String> columnNames = new LinkedList<String>();
		Field[] declaredFields = this.getClass().getDeclaredFields();
		Field idField = null;
		for (Field field : declaredFields) {
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Column){
					Column column = (Column)annotation;
					String columnName = column.name();
					columnNames.add(columnName);
				}
				if(annotation instanceof Id){
					idField = field;
				}
			}
		}
		StringBuilder sql = new StringBuilder("DELETE FROM ").append(tableName);
		if(idField != null){
			Column column = idField.getDeclaredAnnotation(Column.class);
			if(column != null){
				String idColumnName = column.name();
				sql.append(" WHERE ").append(idColumnName).append(" = ?");
			}
		}
		return sql.toString();
		
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
		UniqueConstraint[] uniqueConstraints = tableAnnotation.uniqueConstraints();
		List<String> uniqueColumnNames = new LinkedList<String>();
		for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
			String[] columnNames = uniqueConstraint.columnNames();
			for (String columnName : columnNames) {
				uniqueColumnNames.add(columnName);
			}
		}
		if(uniqueColumnNames.isEmpty()){
			Field[] declaredFields = this.getClass().getDeclaredFields();
			for (Field field : declaredFields) {
				String columnName = "";
				boolean isId = false;
				Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
				for (Annotation annotation : declaredAnnotations) {
					if(annotation instanceof Id){
						isId = true;
					}
					if(annotation instanceof Column){
						Column column = (Column)annotation;
						columnName = column.name();
					}
				}
				if(isId && !"".equals(columnName)){
					uniqueColumnNames.add(columnName);
				}
			}
		}
		String tableName = tableAnnotation.name();
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT ");
		for (String columnName : uniqueColumnNames) {
			sql.append(columnName).append(",");
		}
		if(sql.length() > 0){
			sql.delete(sql.length() - 1, sql.length());
		}
		sql.append(" FROM ").append(tableName).append(" WHERE ");
		for (String columnName : uniqueColumnNames) {
			sql.append(columnName).append(" = ? AND ");
		}
		if(sql.length() > 0){
			sql.delete(sql.length() - 4, sql.length());
		}
		return sql.toString();
		
	}
	
	public List<Object> getExistList(){
		List<Object> list = new LinkedList<Object>();
		Object[] paramArray = getUniqueConstraintsArray();
		for (Object object : paramArray) {
			list.add(object);
		}
		return list;
	}
	
	public String getUniqueConstraintsMD5(){
		StringBuilder sb = new StringBuilder();
		Object[] existArray = getUniqueConstraintsArray();
		for (int i = 0; i < existArray.length; i++) {
			sb.append(existArray[i]).append("|");
		}
		return DigestUtils.md5Hex(sb.toString());
	}
	
	public Object[] getUniqueConstraintsArray(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		UniqueConstraint[] uniqueConstraints = tableAnnotation.uniqueConstraints();
		List<String> uniqueColumnNames = new LinkedList<String>();
		for (UniqueConstraint uniqueConstraint : uniqueConstraints) {
			String[] columnNames = uniqueConstraint.columnNames();
			for (String columnName : columnNames) {
				uniqueColumnNames.add(columnName);
			}
		}
		
		Object[] params = new Object[]{};
		if(uniqueColumnNames.isEmpty()){
			Field[] declaredFields = this.getClass().getDeclaredFields();
			for (Field field : declaredFields) {
				String columnName = "";
				boolean isId = false;
				Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
				for (Annotation annotation : declaredAnnotations) {
					if(annotation instanceof Id){
						isId = true;
					}
					if(annotation instanceof Column){
						Column column = (Column)annotation;
						columnName = column.name();
					}
				}
				if(isId && !"".equals(columnName)){
					params = new Object[]{getValue(field)};
				}
			}
		} else {
			params  = new Object[uniqueColumnNames.size()];
			Field[] declaredFields = this.getClass().getDeclaredFields();
			int idx = 0;
			for (String uniqueColumnName : uniqueColumnNames) {
				Field uniqueColumnField = null;
				for (Field field : declaredFields) {
					Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
					for (Annotation annotation : declaredAnnotations) {
						if(annotation instanceof Column){
							Column column = (Column)annotation;
							if(column.name().equals(uniqueColumnName)){
								uniqueColumnField = field;
							}
						}
					}
				}
				if(uniqueColumnField != null){
					params[idx++] = getValue(uniqueColumnField);
				}
			}
			
		}
		return params;
		
	}
	
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getParamList()
	 */
	
	public List<Object> getParamList(){
		List<Object> list = new LinkedList<Object>();
		Object[] paramArray = getSaveArray();
		for (Object object : paramArray) {
			list.add(object);
		}
		return list;
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getParamArray()
	 */
	
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
				sql.append(column.name()).append(",");
				String sequenceName = getSequenceName(field);
				values.append(sequenceName).append(".NEXTVAL,");
				
			} else {
				sql.append(column.name()).append(",");
				values.append("?,");
			}
		}
		
		Collection<Field> idFields = columnFieldIdMap.values();
		for (Entry<Column,Field> entry : columnFieldMap.entrySet()) {
			Column column = entry.getKey();
			Field field = entry.getValue();
			
			if(!idFields.contains(field)){
				if(isUseDbTime(column)){
					sql.append(column.name()).append(",");
					values.append("#timestamp#,");
				} else {
					sql.append(column.name()).append(",");
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
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getUpdateByIdList()
	 */
	
	public List<Object> getUpdateByUniqueConstraintsList(){
		List<Object> list = new LinkedList<Object>();
		Object[] paramArray = getUpdateByUniqueConstraintsArray();
		for (Object object : paramArray) {
			list.add(object);
		}
		return list;
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getUpdateByIdArray()
	 */
	
	public Object[] getUpdateByUniqueConstraintsArray(){
		List<Field> updateColumnField = new LinkedList<Field>();
		Field[] declaredFields = this.getClass().getDeclaredFields();
		for (Field field : declaredFields) {
			boolean isId = false;
			boolean isUseDbTime = false;
			boolean isUpdatable = true;
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Id){
					isId = true;
				}
				if(annotation instanceof Column){
					Column column = (Column)annotation;
					isUpdatable = column.updatable();
					String columnDefinition = column.columnDefinition();
					if(columnDefinition.toUpperCase().contains("CURRENT_TIMESTAMP")
							|| columnDefinition.toUpperCase().contains("SYSDATE")){
						isUseDbTime = true;
					}
				}
			}
			if(!isId && isUpdatable){
				if(!isUseDbTime){
					updateColumnField.add(field);
				}
			}
		}
		Object[] uniqueConstraintsArray = getUniqueConstraintsArray();
		Object[] params = new Object[updateColumnField.size() + uniqueConstraintsArray.length]; 
		for (int i = 0; i < updateColumnField.size(); i++) {
			params[i] = getValue(updateColumnField.get(i));
			
		}
		for (int i = 0; i < uniqueConstraintsArray.length; i++) {
			params[updateColumnField.size() + i] = uniqueConstraintsArray[i];
			
		}
		return params;
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getDeleteByIdList()
	 */
	
	public List<Object> getDeleteByIdList(){
		List<Object> list = new LinkedList<Object>();
		Object[] paramArray = getDeleteByIdArray();
		for (Object object : paramArray) {
			list.add(object);
		}
		return list;
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getDeleteByIdArray()
	 */
	
	public Object[] getDeleteByIdArray(){
		List<Field> columnField = new LinkedList<Field>();
		Field idField = null;
		for (Field field : this.getClass().getDeclaredFields()) {
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Column){
					columnField.add(field);
				}
				if(annotation instanceof Id){
					idField = field;
				}
			}
		}
		Object[] params = new Object[1];
		params[0] = getValue(idField);
		return params;
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
