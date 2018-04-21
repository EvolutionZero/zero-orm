package com.zero.orm.core;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseDbOperate<T> {

	private static final Logger LOG = LoggerFactory.getLogger(BaseDbOperate.class);
	
	public abstract Connection getConnection();
	
	private Set<String> columnNames = new HashSet<String>();
	
	@SuppressWarnings("rawtypes")
	private ResultSetHandler handler = new ResultSetHandler<List<T>>(){
		@SuppressWarnings("unchecked")
		@Override
		public List<T> handle(ResultSet rs) throws SQLException {
			LinkedList<T> pojos = new LinkedList<T>();
			Class<?> clazz = getGenericClass();
			if(clazz != null && columnNames.isEmpty()){
				Field[] declaredFields = clazz.getDeclaredFields();
				for (Field field : declaredFields) {
					Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
					for (Annotation annotation : declaredAnnotations) {
						if(annotation instanceof Column){
							columnNames.add(field.getName());
						}
					}
				}
			}
			
			Set<String> rowColumnNames = new HashSet<String>();
			ResultSetMetaData metaData = rs.getMetaData();
			for (int i = 1; i <= metaData.getColumnCount(); i++) {
				String rowColumnName = metaData.getColumnName(i);
				rowColumnNames.add(rowColumnName);
			}
			while(rs.next()){
				Map<String, Object> kvs = new HashMap<String, Object>();
				for (Iterator<String> iterator = columnNames.iterator(); iterator.hasNext(); ) {
					String columnName = iterator.next();
					if(rowColumnNames.contains(columnName)){
						Object value = rs.getObject(columnName);
						kvs.put(columnName, value);
					}
				}
				try {
					Constructor<?> constructor = clazz.getDeclaredConstructor(Map.class);
					T pojo = (T)constructor.newInstance(kvs);
					pojos.add(pojo);
				} catch (NoSuchMethodException e) {
					LOG.error("", e);
				} catch (SecurityException e) {
					LOG.error("", e);
				} catch (InstantiationException e) {
					LOG.error("", e);
				} catch (IllegalAccessException e) {
					LOG.error("", e);
				} catch (IllegalArgumentException e) {
					LOG.error("", e);
				} catch (InvocationTargetException e) {
					LOG.error("", e);
				}
			}
			return pojos;
		}
	};
	
	private ResultSetHandler<List<Map<String, Object>>> listMapHandler = new ResultSetHandler<List<Map<String, Object>>>(){

		@Override
		public List<Map<String, Object>> handle(ResultSet rs)
				throws SQLException {
			List<Map<String, Object>> resultList = new LinkedList<Map<String, Object>>();
			ResultSetMetaData metaData = rs.getMetaData();
			List<String> columnNames = new LinkedList<>();
			for (int i = 0; i < metaData.getColumnCount(); i++) {
				String columnName = metaData.getColumnName(i + 1);
				columnNames.add(columnName);
			}
			while(rs.next()){
				Map<String, Object> map = new HashMap<>();
				for (String columnName : columnNames) {
					Object value = rs.getObject(columnName);
					map.put(columnName, value);
				}
				resultList.add(map);
			}
			return resultList;
		}
	};
	
	
	@SuppressWarnings("unchecked")
	public List<T> query(String sql){
		long start = System.currentTimeMillis();
		Connection connection = getConnection();
		List<T> pojos = new LinkedList<T>();
		try {
			pojos = (List<T>)new QueryRunner().query(connection, sql, handler);
		} catch (Exception e) {
			LOG.error("", e);
		} finally {
			DbUtils.closeQuietly(connection);
			long end = System.currentTimeMillis();
			
			StringBuilder info = new StringBuilder("\n");
			info.append("执行SQL: \n").append(sql).append("\n");
			info.append("执行耗时: ").append(end - start).append(" ms.");
			LOG.info(info.toString());
		}
		return pojos;
	}
	
	@SuppressWarnings("unchecked")
	public List<T> query(String sql, Object[] params){
		List<T> pojos = new LinkedList<T>();
		if(params == null || params.length == 0){
			return pojos;
		}
		long start = System.currentTimeMillis();
		Connection connection = getConnection();
		try {
			pojos = (List<T>)new QueryRunner().query(connection, sql, handler, params);
		} catch (Exception e) {
			LOG.error("", e);
		} finally {
			DbUtils.closeQuietly(connection);
			long end = System.currentTimeMillis();
			
			StringBuilder info = new StringBuilder("\n");
			info.append("执行SQL: \n").append(sql).append("\n");
			info.append("设置参数: \n").append(printf(params)).append("\n");
			info.append("执行耗时: ").append(end - start).append(" ms.");
			LOG.info(info.toString());
		}
		return pojos;
	}
	
	public List<Map<String, Object>> queryForList(String sql){
		long start = System.currentTimeMillis();
		Connection connection = getConnection();
		List<Map<String, Object>> pojos = new LinkedList<Map<String, Object>>();
		try {
			pojos = (List<Map<String, Object>>)new QueryRunner().query(connection, sql, listMapHandler);
		} catch (Exception e) {
			LOG.error("", e);
		} finally {
			DbUtils.closeQuietly(connection);
			long end = System.currentTimeMillis();
			
			StringBuilder info = new StringBuilder("\n");
			info.append("执行SQL: \n").append(sql).append("\n");
			info.append("执行耗时: ").append(end - start).append(" ms.");
			LOG.info(info.toString());
		}
		return pojos;
	}
	
	public List<Map<String, Object>> queryForList(String sql, Object[] params){
		List<Map<String, Object>> pojos = new LinkedList<Map<String, Object>>();
		if(params == null || params.length == 0){
			return pojos;
		}
		long start = System.currentTimeMillis();
		Connection connection = getConnection();
		try {
			pojos = (List<Map<String, Object>>)new QueryRunner().query(connection, sql, listMapHandler, params);
		} catch (Exception e) {
			LOG.error("", e);
		} finally {
			DbUtils.closeQuietly(connection);
			long end = System.currentTimeMillis();
			
			StringBuilder info = new StringBuilder("\n");
			info.append("执行SQL: \n").append(sql).append("\n");
			info.append("设置参数: \n").append(printf(params)).append("\n");
			info.append("执行耗时: ").append(end - start).append(" ms.");
			LOG.info(info.toString());
		}
		return pojos;
	}
	
	public int delete(String sql, Object[] params){
		return update(sql, params);
	}
	
	public int[] delete(String sql, Object[][] params){
		return update(sql, params);
	}
	
	public int insert(String sql, Object[] params){
		return update(sql, params);
	}
	
	public int[] insert(String sql, Object[][] params){
		return update(sql, params);
	}
	
	public int[] update(String sql, Object[][] params){
		if(params == null || params.length == 0){
			return new int[]{};
		}
		long start = System.currentTimeMillis();
		Connection connection = getConnection();
		try {
			return new QueryRunner().batch(connection, sql, params);
		} catch (SQLException e) {
			LOG.error("", e);
		} finally {
			DbUtils.closeQuietly(connection);
			long end = System.currentTimeMillis();
			
			StringBuilder info = new StringBuilder("\n");
			info.append("执行SQL: \n").append(sql).append("\n");
			info.append("设置参数: \n").append(printf(params)).append("\n");
			info.append("执行耗时: ").append(end - start).append(" ms.");
			LOG.info(info.toString());
		}
		return new int[]{};
	}
	
	public int update(String sql, Object[] params){
		if(params == null || params.length == 0){
			return 0;
		}
		long start = System.currentTimeMillis();
		Connection connection = getConnection();
		try {
			return new QueryRunner().update(connection, sql, params);
		} catch (SQLException e) {
			LOG.error("", e);
		} finally {
			DbUtils.closeQuietly(connection);
			long end = System.currentTimeMillis();
			StringBuilder info = new StringBuilder("\n");
			info.append("执行SQL: \n").append(sql).append("\n");
			info.append("设置参数: \n").append(printf(params)).append("\n");
			info.append("执行耗时: ").append(end - start).append(" ms.");
			LOG.info(info.toString());
		}
		return 0;
	}
	

	private String printf(Object[][] params){
		StringBuilder sb = new StringBuilder();
		for (Object[] object : params) {
			sb.append(printf(object)).append("\n");
		}
		if(sb.length() > 1){
			sb.delete(sb.length() - 1, sb.length());
		}
		return sb.toString();
	}
	
	private String printf(Object[] params){
		StringBuilder sb = new StringBuilder();
		for (Object object : params) {
			sb.append(object).append(",");
		}
		if(sb.length() > 1){
			sb.delete(sb.length() - 1, sb.length());
		}
		return sb.toString();
	}
	
	protected Class<?> getGenericClass(){
		ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();  
		Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();  
		try {
			return actualTypeArguments.length > 0 ? Class.forName(actualTypeArguments[0].getTypeName()) : null;
			
		} catch (ClassNotFoundException e) {
			LOG.error("", e);
		}
		return null;
	}
}
