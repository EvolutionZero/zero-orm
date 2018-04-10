package com.zero.orm.core;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class PojoBaseBean {
	
	private static final Logger LOG = LoggerFactory.getLogger(PojoBaseBean.class);
	
	public PojoBaseBean(){
	}
	
	public PojoBaseBean(Map<String, Object> resultSet){
		Field[] declaredFields = this.getClass().getDeclaredFields();
		for (Field field : declaredFields) {
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Column){
					Column column = (Column)annotation;
					String columnName = column.name();
					Object value = resultSet.get(columnName) == null ? null : resultSet.get(columnName);
					PropertyDescriptor pd;
					try {
						pd = new PropertyDescriptor(
								field.getName(), this.getClass());
						Method writeMethod = pd.getWriteMethod();
						writeMethod.invoke(this, value);
					} catch (IntrospectionException e) {
						LOG.error("", e);
						
					} catch (IllegalAccessException e) {
						LOG.error("", e);
						
					} catch (IllegalArgumentException e) {
						LOG.error("", e);
						
					} catch (InvocationTargetException e) {
						LOG.error("", e);
						
					}
				}
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
	
	public String getSaveSql(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		
		List<String> columnNames = new LinkedList<String>();
		Field[] declaredFields = this.getClass().getDeclaredFields();
		StringBuilder sql = new StringBuilder("INSERT INTO ");
		sql.append(tableName).append("(");
		StringBuilder values = new StringBuilder();
		for (Field field : declaredFields) {
			String columnName = "";
			boolean isId = false;
			boolean isIdentityStrategy = false;
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Id){
					isId = true;
				}
				if(annotation instanceof GeneratedValue){
					GeneratedValue generatedValue = (GeneratedValue)annotation;
					if(generatedValue.strategy() == GenerationType.IDENTITY){
						isIdentityStrategy = true;
					}
				}
				if(annotation instanceof Column){
					Column column = (Column)annotation;
					columnName = column.name();
				}
			}
			if(!(isId == true && isIdentityStrategy == true)){
				columnNames.add(columnName);
				sql.append(columnName).append(",");
				values.append("?,");
			}
		}
		if(sql.toString().endsWith(",")){
			sql.delete(sql.length() - 1, sql.length());
			values.delete(values.length() - 1, values.length());
		}
		sql.append(") VALUES (").append(values).append(")");
		return sql.toString();
		
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getUpdateSql()
	 */
	
	public String getUpdateSql(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		
		List<String> columnNames = new LinkedList<String>();
		Field[] declaredFields = this.getClass().getDeclaredFields();
		for (Field field : declaredFields) {
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			boolean isId = false;
			String columnName = "";
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Id){
					isId = true;
				}
				if(annotation instanceof Column){
					Column column = (Column)annotation;
					if(column.updatable()){
						columnName = column.name();
					}
				}
			}
			if(!isId && !"".equals(columnName)){
				columnNames.add(columnName);
			}
		}
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(tableName).append(" SET ");
		for (String columnName : columnNames) {
			sql.append(columnName).append(" = ?,");
		}
		if(sql.toString().endsWith(",")){
			sql.delete(sql.length() - 1, sql.length());
		}
		return sql.toString();
		
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getUpdateSqlById()
	 */
	
	public String getUpdateSqlById(){
		Table tableAnnotation = this.getClass().getDeclaredAnnotation(Table.class);
		String tableName = tableAnnotation.name();
		
		List<String> columnNames = new LinkedList<String>();
		Field[] declaredFields = this.getClass().getDeclaredFields();
		Field idField = null;
		for (Field field : declaredFields) {
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			boolean isId = false;
			String columnName = "";
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Id){
					isId = true;
				}
				if(annotation instanceof Column){
					Column column = (Column)annotation;
					if(column.updatable()){
						columnName = column.name();
					}
				}
				if(annotation instanceof Id){
					idField = field;
				}
			}
			if(!isId && !"".equals(columnName)){
				columnNames.add(columnName);
			}
		}
		
		StringBuilder sql = new StringBuilder("UPDATE ");
		sql.append(tableName).append(" SET ");
		for (String columnName : columnNames) {
			sql.append(columnName).append(" = ?,");
		}
		if(sql.toString().endsWith(",")){
			sql.delete(sql.length() - 1, sql.length());
		}
		
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
		StringBuilder sql = new StringBuilder();
		if(idField != null){
			Column column = idField.getDeclaredAnnotation(Column.class);
			if(column != null){
				String idColumnName = column.name();
				sql.append(" SELECT ").append(idColumnName).append(" FROM ").append(tableName).append(" WHERE ")
				.append(idColumnName);
			}
		}
		return sql.toString();
		
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
	
	public Object[] getSaveArray(){
		List<Field> columnField = new LinkedList<Field>();
		for (Field field : this.getClass().getDeclaredFields()) {
			boolean isId = false;
			boolean isIdentityStrategy = false;
			boolean isColumn = false;
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Id){
					isId = true;
				}
				if(annotation instanceof GeneratedValue){
					GeneratedValue generatedValue = (GeneratedValue)annotation;
					if(generatedValue.strategy() == GenerationType.IDENTITY){
						isIdentityStrategy = true;
					}
				}
				if(annotation instanceof Column){
					isColumn = true;
				}
			}
			if(isColumn && !(isId == true && isIdentityStrategy == true)){
				columnField.add(field);
			}
		}
		Object[] params = new Object[columnField.size()];
		int idx = 0;
		for (Field field : columnField) {
			PropertyDescriptor pd;
			try {
				pd = new PropertyDescriptor(
						field.getName(), this.getClass());
				Method getMethod = pd.getReadMethod();// 获得get方法
				Object param = getMethod.invoke(this);
				params[idx++] = param;
			} catch (IntrospectionException e) {
				LOG.error("", e);
				
			} catch (IllegalAccessException e) {
				LOG.error("", e);
				
			} catch (IllegalArgumentException e) {
				LOG.error("", e);
				
			} catch (InvocationTargetException e) {
				LOG.error("", e);
				
			}
		}
		return params;
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getUpdateByIdList()
	 */
	
	public List<Object> getUpdateByIdList(){
		List<Object> list = new LinkedList<Object>();
		Object[] paramArray = getUpdateByIdArray();
		for (Object object : paramArray) {
			list.add(object);
		}
		return list;
	}
	
	/* (non-Javadoc)
	 * @see com.catt.tsdn.collect.bean.pojo.IPojo#getUpdateByIdArray()
	 */
	
	public Object[] getUpdateByIdArray(){
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
		Object[] params = new Object[columnField.size() + 1];
		int idx = 0;
		for (Field field : columnField) {
			PropertyDescriptor pd;
			try {
				pd = new PropertyDescriptor(
						field.getName(), this.getClass());
				Method getMethod = pd.getReadMethod();// 获得get方法
				Object param = getMethod.invoke(this);
				params[idx++] = param;
			} catch (IntrospectionException e) {
				LOG.error("", e);
				
			} catch (IllegalAccessException e) {
				LOG.error("", e);
				
			} catch (IllegalArgumentException e) {
				LOG.error("", e);
				
			} catch (InvocationTargetException e) {
				LOG.error("", e);
				
			}
		}
		try {
			PropertyDescriptor pd = new PropertyDescriptor(
					idField.getName(), this.getClass());
			Method getMethod = pd.getReadMethod();// 获得get方法
			Object id = getMethod.invoke(this);
			params[columnField.size()] = id;
		} catch (IntrospectionException e) {
			LOG.error("", e);
			
		} catch (IllegalAccessException e) {
			LOG.error("", e);
			
		} catch (IllegalArgumentException e) {
			LOG.error("", e);
			
		} catch (InvocationTargetException e) {
			LOG.error("", e);
			
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
		try {
			PropertyDescriptor pd = new PropertyDescriptor(
					idField.getName(), this.getClass());
			Method getMethod = pd.getReadMethod();// 获得get方法
			Object id = getMethod.invoke(this);
			params[0] = id;
		} catch (IntrospectionException e) {
			LOG.error("", e);
			
		} catch (IllegalAccessException e) {
			LOG.error("", e);
			
		} catch (IllegalArgumentException e) {
			LOG.error("", e);
			
		} catch (InvocationTargetException e) {
			LOG.error("", e);
			
		}
		return params;
	}
	
}
