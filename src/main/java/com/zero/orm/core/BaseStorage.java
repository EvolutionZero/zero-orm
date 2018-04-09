//package com.zero.orm.core;
//
//import java.beans.IntrospectionException;
//import java.beans.PropertyDescriptor;
//import java.lang.annotation.Annotation;
//import java.lang.reflect.Field;
//import java.lang.reflect.InvocationTargetException;
//import java.lang.reflect.Method;
//import java.lang.reflect.ParameterizedType;
//import java.lang.reflect.Type;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.Collection;
//import java.util.HashSet;
//import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//
//import javax.persistence.Column;
//import javax.persistence.Id;
//import javax.persistence.Table;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public abstract class BaseStorage<T extends PojoBaseBean> {
//
//	private static final Logger LOG = LoggerFactory.getLogger(BaseStorage.class);
//	
//	protected String tableName;
//	protected String idColumnName;
//	protected Field idField;
//	
//	protected String query;
//	protected String save;
//	protected String updateById;
//	protected String deleteById;
//	
//	private String dataSourceId;
//	
//	public BaseStorage(String dataSourceId){
//		this.dataSourceId = dataSourceId;
//		ParameterizedType parameterizedType = (ParameterizedType) this.getClass().getGenericSuperclass();  
//		Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();  
//		try {
//			Class<?> clazz = actualTypeArguments.length > 0 ? Class.forName(actualTypeArguments[0].getTypeName()) : null;
//			Table tableAnnotation = clazz.getDeclaredAnnotation(Table.class);
//			tableName = tableAnnotation.name();
//			
//			Field[] declaredFields = clazz.getDeclaredFields();
//			for (Field field : declaredFields) {
//				Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
//				for (Annotation annotation : declaredAnnotations) {
//					if(annotation instanceof Id){
//						idField = field;
//						break;
//					}
//				}
//			}
//			
//			PojoBaseBean bean = (PojoBaseBean)clazz.newInstance();
//			query = bean.getQuerySql();
//			save = bean.getSaveSql();
//			updateById = bean.getUpdateSqlById();
//			deleteById = bean.getDeleteByIdSql();
//		} catch (ClassNotFoundException e) {
//			LOG.error("", e);
//		} catch (InstantiationException e) {
//			LOG.error("", e);
//		} catch (IllegalAccessException e) {
//			LOG.error("", e);
//		}
//		if(idField != null){
//			Column column = idField.getDeclaredAnnotation(Column.class);
//			idColumnName = column.name();
//		}
//	}
//	
//	public void saveOrUpdate(List<T> datas){
//		if(datas.isEmpty()){
//			return ;
//		}
//		List<Object> collectKey = new LinkedList<Object>();
//		List<Object[]> params = new LinkedList<Object[]>();
//		for (T data : datas) {
//			params.add(data.getParamArray());
//			collectKey.add(getIdValue(data));
//		}
//		List<String> dbKeys = getKeys(collectKey);
//		
//		List<T> saveDatas = new LinkedList<T>();
//		List<T> updateDatas = new LinkedList<T>();
//		for (T data : datas) {
//			if(dbKeys.contains(getIdValue(data))){
//				updateDatas.add(data);
//			} else {
//				saveDatas.add(data);
//			}
//		}
//		insert(save, getSaveParams(saveDatas));
//		update(updateById, getUpdateByIdParams(updateDatas));
//	}
//	
//	public void saveOnly(List<T> datas){
//		if(datas.isEmpty()){
//			return ;
//		}
//		List<Object> collectKey = new LinkedList<Object>();
//		List<Object[]> params = new LinkedList<Object[]>();
//		for (T data : datas) {
//			params.add(data.getParamArray());
//			collectKey.add(getIdValue(data));
//		}
//		List<String> dbKeys = getKeys(collectKey);
//		
//		List<T> saveDatas = new LinkedList<T>();
//		for (T data : datas) {
//			if(!dbKeys.contains(getIdValue(data))){
//				saveDatas.add(data);
//			}
//		}
//		insert(save, getSaveParams(saveDatas));
//	}
//	
//	public Object[][] getSaveParams(List<T> datas){
//		Object[][] params = new Object[datas.size()][];
//		int idx = 0;
//		for (T node : datas) {
//			params[idx++] = node.getParamArray();
//		}
//		return params;
//	}
//	
//	public Object[][] getUpdateByIdParams(List<T> datas){
//		Object[][] params = new Object[datas.size()][];
//		int idx = 0;
//		for (T node : datas) {
//			params[idx++] = node.getUpdateByIdArray();
//		}
//		return params;
//	}
//	
//	protected List<String> getKeys(Collection<Object> datas){
//		List<String> result = new LinkedList<String>();
//		if(datas.isEmpty()){
//			return result;
//		}
//		// 首先去重
//		Set<Object> onlyDatas = new HashSet<Object>(datas);
//		HashSet<Object> cache = new HashSet<Object>();
//		// ORACLE的IN条件最大值为1000
//		int cacheMaxSize = 1000;
//		for (Object data : onlyDatas) {
//			cache.add(data);
//			if(cache.size() % cacheMaxSize == 0){
//				result.addAll(getExistKeys(cache));
//				cache.clear();
//			}
//		}
//		result.addAll(getExistKeys(cache));
//		return result;
//	}
//	
//	private List<String> getExistKeys(Collection<Object> datas){
//		List<String> result = new LinkedList<String>();
//		if(datas.isEmpty()){
//			return result;
//		}
//		Iterator<Object> iterator = datas.iterator();
//		Object check = iterator.next();
//		String condition = "";
//		if(check != null && check instanceof String){
//			condition = toInList(datas);
//		} else {
//			condition = toNumbericInList(datas);
//		}
//		String sql = "SELECT " + idColumnName + " FROM " + tableName 
//				+ " WHERE " + idColumnName +  " IN (" + condition + ")";
//		List<Map<String,Object>> query = query(sql);
//		for (Map<String,Object> map : query) {
//			String key = map.get(idColumnName) == null ? "" : (String)map.get(idColumnName);
//			result.add(key);
//		}
//		return result;
//	}
//	
//	private Object getIdValue(T bean){
//		Object value = null;
//		try {
//			PropertyDescriptor pd = new PropertyDescriptor(
//					idField.getName(), bean.getClass());
//			Method getMethod = pd.getReadMethod();// 获得get方法
//			value = getMethod.invoke(bean);
//		} catch (IllegalAccessException e) {
//			LOG.error("", e);
//		} catch (IllegalArgumentException e) {
//			LOG.error("", e);
//		} catch (InvocationTargetException e) {
//			LOG.error("", e);
//		} catch (IntrospectionException e) {
//			LOG.error("", e);
//		}
//		return value;
//	}
//	
//	public void deleteById(T bean){
//		Object idValue = getIdValue(bean);
//		delete(deleteById, new Object[]{idValue});
//	}
//	
//	protected List<Map<String,Object>> query(String sql){
//		LOG.info(sql);
//		Connection connection = null;
//		List<Map<String,Object>> result = new LinkedList<Map<String,Object>>();
//		try {
//			connection = DB.getConnection(dataSourceId);
//			result = DB.query(connection, sql);
//		} catch (Exception e) {
//			LOG.error("", e);
//		} finally {
//			DB.close(connection);
//		}
//		return result;
//	}
//	
//	protected List<Map<String,Object>> query(String sql, Object[][] params){
//		Connection connection = null;
//		List<Map<String,Object>> result = new LinkedList<Map<String,Object>>();
//		if(params == null || params.length == 0){
//			return result;
//		}
//		LOG.info(sql);
//		LOG.info(printf(params));
//		try {
//			connection = DB.getConnection(dataSourceId);
//			result = DB.query(connection, sql, params);
//		} catch (Exception e) {
//			LOG.error("", e);
//		} finally {
//			DB.close(connection);
//		}
//		return result;
//	}
//	
//	
//	protected List<Map<String,Object>> query(String sql, Object[] params){
//		List<Map<String,Object>> result = new LinkedList<Map<String,Object>>();
//		if(params == null || params.length == 0){
//			return result;
//		}
//		LOG.info(sql);
//		LOG.info(printf(params));
//		Connection connection = null;
//		try {
//			connection = DB.getConnection(dataSourceId);
//			result = DB.query(connection, sql, params);
//		} catch (Exception e) {
//			LOG.error("", e);
//		} finally {
//			DB.close(connection);
//		}
//		return result;
//	}
//	
//	protected void delete(String sql, Object[] params){
//		if(params == null || params.length == 0){
//			return ;
//		}
//		LOG.info(sql);
//		LOG.info(printf(params));
//		Connection connection = null;
//		try {
//			connection = DB.getConnection(dataSourceId);
//			DB.update(connection, sql , params);
//		} catch (SQLException e) {
//			LOG.error("", e);
//		} catch (ClassNotFoundException e) {
//			LOG.error("", e);
//		} finally {
//			DB.close(connection);
//		}
//	}
//	
//	protected void delete(String sql, Object[][] parms){
//		if(parms == null || parms.length == 0){
//			return ;
//		}
//		LOG.info(sql);
//		LOG.info(printf(parms));
//		Connection connection = null;
//		try {
//			connection = DB.getConnection(dataSourceId);
//			DB.update(connection, sql , parms);
//		} catch (SQLException e) {
//			LOG.error("", e);
//		} catch (ClassNotFoundException e) {
//			LOG.error("", e);
//		} finally {
//			DB.close(connection);
//		}
//	}
//	
//	protected int update(String sql, Object[][] parms){
//		int update = 0;
//		if(parms == null || parms.length == 0){
//			return update;
//		}
//		LOG.info(sql);
//		LOG.info(printf(parms));
//		Connection connection = null;
//		try {
//			connection = DB.getConnection(dataSourceId);
//			update = DB.update(connection, sql , parms).length;
//		} catch (SQLException e) {
//			LOG.error("", e);
//		} catch (ClassNotFoundException e) {
//			LOG.error("", e);
//		} finally {
//			DB.close(connection);
//		}
//		return update;
//	}
//	
//	protected int insert(String sql, Object[] parms){
//		int update = 0;
//		if(parms == null || parms.length == 0){
//			return update;
//		}
//		LOG.info(sql);
//		LOG.info(printf(parms));
//		Connection connection = null;
//		try {
//			connection = DB.getConnection(dataSourceId);
//			update = DB.update(connection, sql , parms);
//		} catch (SQLException e) {
//			LOG.error("", e);
//		} catch (ClassNotFoundException e) {
//			LOG.error("", e);
//		} finally {
//			DB.close(connection);
//		}
//		return update;
//	}
//	
//	protected int insert(String sql, Object[][] parms){
//		int update = 0;
//		if(parms == null || parms.length == 0){
//			return update;
//		}
//		LOG.info(sql);
//		LOG.info(printf(parms));
//		Connection connection = null;
//		try {
//			connection = DB.getConnection(dataSourceId);
//			update = DB.update(connection, sql , parms).length;
//		} catch (SQLException e) {
//			LOG.error("", e);
//		} catch (ClassNotFoundException e) {
//			LOG.error("", e);
//		} finally {
//			DB.close(connection);
//		}
//		return update;
//	}
//	
//	protected String toInList(Collection<Object> datas){
//		StringBuilder sb = new StringBuilder();
//		for (Object data : datas) {
//			sb.append("'").append(data).append("'").append(",");
//		}
//		if(sb.length() > 1){
//			sb.delete(sb.length() - 1, sb.length());
//		}
//		return "".equals(sb.toString()) ? "''" : sb.toString();
//	}
//	
//	protected String toNumbericInList(Collection<Object> datas){
//		StringBuilder sb = new StringBuilder();
//		for (Object data : datas) {
//			sb.append(data).append(",");
//		}
//		if(sb.length() > 1){
//			sb.delete(sb.length() - 1, sb.length());
//		}
//		return "".equals(sb.toString()) ? "''" : sb.toString();
//	}
//	
//	private String printf(Object[][] params){
//		StringBuilder sb = new StringBuilder("\n");
//		for (Object[] object : params) {
//			sb.append(printf(object));
//		}
//		return sb.toString();
//	}
//	
//	private String printf(Object[] params){
//		StringBuilder sb = new StringBuilder("\n");
//		for (Object object : params) {
//			sb.append(object).append(",");
//		}
//		if(sb.length() > 1){
//			sb.delete(sb.length() - 1, sb.length());
//		}
//		return sb.toString();
//	}
//	
//}
