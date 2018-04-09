package com.zero.orm.core;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseStorage<T extends PojoBaseBean> {

	private static final Logger LOG = LoggerFactory.getLogger(BaseStorage.class);
	
	protected String tableName;
	protected String idColumnName;
	protected Field idField;
	
	protected String query;
	protected String save;
	protected String updateById;
	protected String deleteById;
	private Set<String> columnNames = new HashSet<String>();
	
	@SuppressWarnings("rawtypes")
	private ResultSetHandler handler = new ResultSetHandler<List<T>>(){
		@SuppressWarnings("unchecked")
		@Override
		public List<T> handle(ResultSet rs) throws SQLException {
			LinkedList<T> pojos = new LinkedList<T>();
			while(rs.next()){
				Map<String, Object> kvs = new HashMap<String, Object>();
				for (Iterator<String> iterator = columnNames.iterator(); iterator.hasNext(); ) {
					String columnName = iterator.next();
					Object value = rs.getObject(columnName);
					kvs.put(columnName, value);
				}
				Class<?> clazz = getGenericClass();
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
	
	public abstract Connection getConnection();
	
	public BaseStorage(){
		Class<?> clazz = getGenericClass();
		if(clazz != null){
			Table tableAnnotation = clazz.getDeclaredAnnotation(Table.class);
			tableName = tableAnnotation.name();
			
			Field[] declaredFields = clazz.getDeclaredFields();
			for (Field field : declaredFields) {
				Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
				for (Annotation annotation : declaredAnnotations) {
					if(annotation instanceof Id){
						idField = field;
					}
					if(annotation instanceof Column){
						columnNames.add(field.getName());
					}
				}
			}
			
			try {
				PojoBaseBean bean = (PojoBaseBean)clazz.newInstance();
				query = bean.getQuerySql();
				save = bean.getSaveSql();
				updateById = bean.getUpdateSqlById();
				deleteById = bean.getDeleteByIdSql();
				if(idField != null){
					Column column = idField.getDeclaredAnnotation(Column.class);
					idColumnName = column.name();
				}
			} catch (InstantiationException e) {
				LOG.error("", e);
			} catch (IllegalAccessException e) {
				LOG.error("", e);
			}
		
		}
	}
	
	private Class<?> getGenericClass(){
		ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();  
		Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();  
		try {
			return actualTypeArguments.length > 0 ? Class.forName(actualTypeArguments[0].getTypeName()) : null;
			
		} catch (ClassNotFoundException e) {
			LOG.error("", e);
		}
		return null;
	}
	
	
	public void saveOrUpdate(List<T> datas){
		if(datas.isEmpty()){
			return ;
		}
		List<Object> collectKey = new LinkedList<Object>();
		List<Object[]> params = new LinkedList<Object[]>();
		for (T data : datas) {
			params.add(data.getParamArray());
			collectKey.add(getIdValue(data));
		}
		List<String> dbKeys = getKeys(collectKey);
		
		List<T> saveDatas = new LinkedList<T>();
		List<T> updateDatas = new LinkedList<T>();
		for (T data : datas) {
			if(dbKeys.contains(getIdValue(data))){
				updateDatas.add(data);
			} else {
				saveDatas.add(data);
			}
		}
		insert(save, getSaveParams(saveDatas));
		update(updateById, getUpdateByIdParams(updateDatas));
	}
	
	public void saveOnly(List<T> datas){
		if(datas.isEmpty()){
			return ;
		}
		List<Object> collectKey = new LinkedList<Object>();
		List<Object[]> params = new LinkedList<Object[]>();
		for (T data : datas) {
			params.add(data.getParamArray());
			collectKey.add(getIdValue(data));
		}
		List<String> dbKeys = getKeys(collectKey);
		
		List<T> saveDatas = new LinkedList<T>();
		for (T data : datas) {
			if(!dbKeys.contains(getIdValue(data))){
				saveDatas.add(data);
			}
		}
		insert(save, getSaveParams(saveDatas));
	}
	
	public Object[][] getSaveParams(List<T> datas){
		Object[][] params = new Object[datas.size()][];
		int idx = 0;
		for (T node : datas) {
			params[idx++] = node.getParamArray();
		}
		return params;
	}
	
	public Object[][] getUpdateByIdParams(List<T> datas){
		Object[][] params = new Object[datas.size()][];
		int idx = 0;
		for (T node : datas) {
			params[idx++] = node.getUpdateByIdArray();
		}
		return params;
	}
	
	protected List<String> getKeys(Collection<Object> datas){
		List<String> result = new LinkedList<String>();
		if(datas.isEmpty()){
			return result;
		}
		// 首先去重
		Set<Object> onlyDatas = new HashSet<Object>(datas);
		HashSet<Object> cache = new HashSet<Object>();
		// ORACLE的IN条件最大值为1000
		int cacheMaxSize = 1000;
		for (Object data : onlyDatas) {
			cache.add(data);
			if(cache.size() % cacheMaxSize == 0){
				result.addAll(getExistKeys(cache));
				cache.clear();
			}
		}
		result.addAll(getExistKeys(cache));
		return result;
	}
	
	private List<String> getExistKeys(Collection<Object> datas){
		List<String> result = new LinkedList<String>();
		if(datas.isEmpty()){
			return result;
		}
		Iterator<Object> iterator = datas.iterator();
		Object check = iterator.next();
		String condition = "";
		if(check != null && check instanceof String){
			condition = toInList(datas);
		} else {
			condition = toNumbericInList(datas);
		}
		String sql = "SELECT " + idColumnName + " FROM " + tableName 
				+ " WHERE " + idColumnName +  " IN (" + condition + ")";
		List<T> pojos = query(sql);
		for (T pojo : pojos) {
			Object idValue = getIdValue(pojo);
			result.add(idValue == null ? "" : idValue.toString());
		}
		return result;
	}
	
	private Object getIdValue(T bean){
		Object value = null;
		try {
			PropertyDescriptor pd = new PropertyDescriptor(
					idField.getName(), bean.getClass());
			Method getMethod = pd.getReadMethod();// 获得get方法
			value = getMethod.invoke(bean);
		} catch (IllegalAccessException e) {
			LOG.error("", e);
		} catch (IllegalArgumentException e) {
			LOG.error("", e);
		} catch (InvocationTargetException e) {
			LOG.error("", e);
		} catch (IntrospectionException e) {
			LOG.error("", e);
		}
		return value;
	}
	
	public void deleteById(T bean){
		Object idValue = getIdValue(bean);
		delete(deleteById, new Object[]{idValue});
	}
	
	@SuppressWarnings("unchecked")
	public List<T> query(String sql){
		LOG.info(sql);
		Connection connection = getConnection();
		List<T> pojos = new LinkedList<T>();
		try {
			pojos = (List<T>)new QueryRunner().query(connection, sql, handler);
		} catch (Exception e) {
			LOG.error("", e);
		} finally {
			DbUtils.closeQuietly(connection);
		}
		return pojos;
	}
	
	@SuppressWarnings("unchecked")
	public List<T> query(String sql, Object[] params){
		List<T> pojos = new LinkedList<T>();
		if(params == null || params.length == 0){
			return pojos;
		}
		LOG.info(sql);
		LOG.info(printf(params));
		Connection connection = getConnection();
		try {
			pojos = (List<T>)new QueryRunner().query(connection, sql, handler, params);
		} catch (Exception e) {
			LOG.error("", e);
		} finally {
			DbUtils.closeQuietly(connection);
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
		LOG.info(sql);
		LOG.info(printf(params));
		Connection connection = getConnection();
		try {
			return new QueryRunner().batch(connection, sql, params);
		} catch (SQLException e) {
			LOG.error("", e);
		} finally {
			DbUtils.closeQuietly(connection);
		}
		return new int[]{};
	}
	
	public int update(String sql, Object[] params){
		if(params == null || params.length == 0){
			return 0;
		}
		LOG.info(sql);
		LOG.info(printf(params));
		Connection connection = getConnection();
		try {
			return new QueryRunner().update(connection, sql, params);
		} catch (SQLException e) {
			LOG.error("", e);
		} finally {
			DbUtils.closeQuietly(connection);
		}
		return 0;
	}
	
	protected String toInList(Collection<Object> datas){
		StringBuilder sb = new StringBuilder();
		for (Object data : datas) {
			sb.append("'").append(data).append("'").append(",");
		}
		if(sb.length() > 1){
			sb.delete(sb.length() - 1, sb.length());
		}
		return "".equals(sb.toString()) ? "''" : sb.toString();
	}
	
	protected String toNumbericInList(Collection<Object> datas){
		StringBuilder sb = new StringBuilder();
		for (Object data : datas) {
			sb.append(data).append(",");
		}
		if(sb.length() > 1){
			sb.delete(sb.length() - 1, sb.length());
		}
		return "".equals(sb.toString()) ? "''" : sb.toString();
	}
	
	private String printf(Object[][] params){
		StringBuilder sb = new StringBuilder("\n");
		for (Object[] object : params) {
			sb.append(printf(object));
		}
		return sb.toString();
	}
	
	private String printf(Object[] params){
		StringBuilder sb = new StringBuilder("\n");
		for (Object object : params) {
			sb.append(object).append(",");
		}
		if(sb.length() > 1){
			sb.delete(sb.length() - 1, sb.length());
		}
		return sb.toString();
	}
	
}

