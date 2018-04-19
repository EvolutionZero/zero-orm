package com.zero.orm.core;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import org.apache.commons.dbutils.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

enum Database{
	MYSQL, ORACLE;
}

public abstract class BaseStorage<T extends PojoBaseBean> extends BaseDbOperate<T>{

	private static final Logger LOG = LoggerFactory.getLogger(BaseStorage.class);
	
	protected String tableName;
	protected String idColumnName;
	protected Field idField;
	
	protected String exist;
	protected String query;
	protected String save;
	protected String updateByUniqueConstraints;
	protected String deleteById;
	
	
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
				}
			}
			
			try {
				PojoBaseBean bean = (PojoBaseBean)clazz.newInstance();
				exist = bean.getExistSql();
				query = bean.getQuerySql();
				save = bean.getSaveSql();
				updateByUniqueConstraints = bean.getUpdateSqlByUniqueConstraints();
				deleteById = bean.getDeleteByIdSql();
				if(idField != null){
					Column column = idField.getDeclaredAnnotation(Column.class);
					idColumnName = column.name();
				}
				
				Connection connection = getConnection();
				try {
					String databaseProductName = connection.getMetaData().getDatabaseProductName();
					if(databaseProductName.toUpperCase().contains(Database.MYSQL.toString())){
						save = save.replace("#timestamp#", "now()");
						updateByUniqueConstraints = updateByUniqueConstraints.replace("#timestamp#", "now()");
					} else if(databaseProductName.toUpperCase().contains(Database.ORACLE.toString())){
						save = save.replace("#timestamp#", "sysdate");
						updateByUniqueConstraints = updateByUniqueConstraints.replace("#timestamp#", "sysdate");
					}
				} catch (SQLException e1) {
					LOG.error("", e1);
				} finally {
					DbUtils.closeQuietly(connection);
				}
			} catch (InstantiationException e) {
				LOG.error("", e);
			} catch (IllegalAccessException e) {
				LOG.error("", e);
			}
		
		}
	}
	
	/**
	 * 只存
	 * @param datas
	 */
	public void saveOnly(List<T> datas){
		if(datas.isEmpty()){
			return ;
		}
		insert(save, getSaveParams(datas));
	}
	
	public void saveOnly(T data){
		LinkedList<T> list = new LinkedList<T>();
		list.add(data);
		saveOnly(list);
	}
	
	/**
	 * 根据主键进行判断，主键存在则跳过不处理
	 * @param datas
	 */
	public void saveUnique(List<T> datas){
		if(datas.isEmpty()){
			return ;
		}
		List<Object> collectKey = new LinkedList<Object>();
		List<Object[]> params = new LinkedList<Object[]>();
		for (T data : datas) {
			params.add(data.getSaveArray());
			collectKey.add(getIdValue(data));
		}
		List<T> existPojos = getExistPojos(datas);
		List<String> dbKeys = new LinkedList<String>();
		for (T pojo : existPojos) {
			dbKeys.add(pojo.getExistMD5());
		}
		
		List<T> saveDatas = new LinkedList<T>();
		for (T data : datas) {
			if(!dbKeys.contains(data.getExistMD5())){
				saveDatas.add(data);
			}
		}
		insert(save, getSaveParams(saveDatas));
	}
	
	public void saveUnique(T data){
		LinkedList<T> list = new LinkedList<T>();
		list.add(data);
		saveUnique(list);
	}
	
	/**
	 * 根据主键进行判断，有则存储，没有则更新
	 * @param datas
	 */
	public void saveOrUpdate(List<T> datas){
		if(datas.isEmpty()){
			return ;
		}
		List<Object[]> params = new LinkedList<Object[]>();
		for (T data : datas) {
			params.add(data.getSaveArray());
		}
		
		List<T> existPojos = getExistPojos(datas);
		List<String> dbKeys = new LinkedList<String>();
		for (T pojo : existPojos) {
			dbKeys.add(pojo.getExistMD5());
		}
		
		List<T> saveDatas = new LinkedList<T>();
		List<T> updateDatas = new LinkedList<T>();
		for (T data : datas) {
			if(dbKeys.contains(data.getExistMD5())){
				updateDatas.add(data);
			} else {
				saveDatas.add(data);
			}
		}
		insert(save, getSaveParams(saveDatas));
		update(updateByUniqueConstraints, getUpdateByUniqueConstraintsParams(updateDatas));
	}
	
	
	public void saveOrUpdate(T data){
		LinkedList<T> list = new LinkedList<T>();
		list.add(data);
		saveOrUpdate(list);
	}
	
	protected Object[][] getSaveParams(List<T> datas){
		Object[][] params = new Object[datas.size()][];
		int idx = 0;
		for (T data : datas) {
			params[idx++] = data.getSaveArray();
		}
		return params;
	}
	
	protected Object[][] getUpdateByUniqueConstraintsParams(List<T> datas){
		Object[][] params = new Object[datas.size()][];
		int idx = 0;
		for (T data : datas) {
			params[idx++] = data.getUpdateByUniqueConstraintsArray();
		}
		return params;
	}
	
	protected List<T> getExistPojos(List<T> datas){
		List<T> result = new LinkedList<T>();
		if(datas.isEmpty()){
			return result;
		}
		// 首先去重
		Set<T> onlyDatas = new HashSet<T>(datas);
		List<T> cache = new LinkedList<T>();
		// ORACLE的IN条件最大值为1000
		int cacheMaxSize = 1000;
		for (T data : onlyDatas) {
			cache.add(data);
			if(cache.size() % cacheMaxSize == 0){
				result.addAll(getOnceExistPojos(cache));
				cache.clear();
			}
		}
		result.addAll(getOnceExistPojos(cache));
		return result;
	}
	
	private List<T> getOnceExistPojos(List<T> datas){
		LinkedList<T> result = new LinkedList<T>();
		if(datas.isEmpty()){
			return new LinkedList<T>();
		}
		if(datas.get(0).getExistArray().length == 1){
			Set<Object> keys = new HashSet<Object>();
			for (T data : datas) {
				keys.add(data.getExistArray()[0]);
			}
			String inCondition = toInList(keys);
			String sql = exist.replace("= ?", "") + " IN (" + inCondition + ")";
			result.addAll(query(sql));
		} else {
			for (T pojo : datas) {
				result.addAll(query(exist, pojo.getExistArray()));
			}
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
	
	protected String toInList(Collection<?> datas){
		StringBuilder sb = new StringBuilder();
		boolean isString = false;
		for (Object data : datas) {
			if(data instanceof String){
				isString = true;
			}
			break;
		}
		if(isString){
			for (Object data : datas) {
				sb.append("'").append(data).append("'").append(",");
			}
			if(sb.length() > 1){
				sb.delete(sb.length() - 1, sb.length());
			}
			return "".equals(sb.toString()) ? "' '" : sb.toString();
			
		} else {
			return toNumbericInList(datas);
		}
	}
	
	private String toNumbericInList(Collection<?> datas){
		StringBuilder sb = new StringBuilder();
		for (Object data : datas) {
			sb.append(data).append(",");
		}
		if(sb.length() > 1){
			sb.delete(sb.length() - 1, sb.length());
		}
		return "".equals(sb.toString()) ? "''" : sb.toString();
	}
	
}
