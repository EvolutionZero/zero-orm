package com.zero.orm.core;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseStorage<T extends PojoBaseBean> extends BaseDbOperate<T>{

	private static final Logger LOG = LoggerFactory.getLogger(BaseStorage.class);
	
	protected String tableName;
	protected String idColumnName;
	protected Field idField;
	
	protected String query;
	protected String save;
	protected String updateById;
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
	
	
	public void saveOrUpdate(T data){
		LinkedList<T> list = new LinkedList<T>();
		list.add(data);
		saveOrUpdate(list);
	}
	
	protected Object[][] getSaveParams(List<T> datas){
		Object[][] params = new Object[datas.size()][];
		int idx = 0;
		for (T data : datas) {
			params[idx++] = data.getParamArray();
		}
		return params;
	}
	
	protected Object[][] getUpdateByIdParams(List<T> datas){
		Object[][] params = new Object[datas.size()][];
		int idx = 0;
		for (T data : datas) {
			params[idx++] = data.getUpdateByIdArray();
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
	
	protected String toInList(Collection<Object> datas){
		StringBuilder sb = new StringBuilder();
		for (Object data : datas) {
			sb.append("'").append(data).append("'").append(",");
		}
		if(sb.length() > 1){
			sb.delete(sb.length() - 1, sb.length());
		}
		return "".equals(sb.toString()) ? "' '" : sb.toString();
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
	
}

