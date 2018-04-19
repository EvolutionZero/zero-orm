package com.zero.orm.core;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.Column;
import javax.persistence.Id;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zero.orm.utils.ClassUtils;

public class ClassStructCache {

	private static final Logger LOG = LoggerFactory.getLogger(ClassStructCache.class);
	
	protected static final Map<Class<?>, Map<Column, Field>> COLUMN_FIELD_MAPPING = new HashMap<>();
	protected static final Map<Class<?>, Map<Field, Column>> FIELD_COLUMN_MAPPING = new HashMap<>();
	
	protected static final Map<Class<?>, Map<Field, Method	>> FIELD_GETTER_MAPPING = new HashMap<>();
	protected static final Map<Class<?>, Map<Field, Method	>> FIELD_SETTER_MAPPING = new HashMap<>();
	
	protected static final Map<Class<?>, Map<Column, Field>> COLUMN_FIELD_ID_MAPPING = new HashMap<>();
	protected static final Map<Class<?>, Map<Field, Column>> FIELD_COLUMN_ID_MAPPING = new HashMap<>();
	
	static{
		List<Class<?>> subClazzs = ClassUtils.findSubClass(PojoBaseBean.class);
		initColumnFieldMapping(subClazzs);
		initFieldColumnMapping(subClazzs);
		initFieldGetterMapping(subClazzs);
		initFieldSetterMapping(subClazzs);
		initColumnFieldIdMapping(subClazzs);
		initFieldColumnIdMapping(subClazzs);
	}

	private static void initColumnFieldMapping(List<Class<?>> clazzs){
		for (Class<?> clazz : clazzs) {
			COLUMN_FIELD_MAPPING.put(clazz, genColumnFieldMap(clazz));
			
		}
	}
	
	private static Map<Column, Field> genColumnFieldMap(Class<?> clazz){
		Map<Column, Field> columnFieldMap = new HashMap<>();
		Field[] declaredFields = clazz.getDeclaredFields();
		for (Field field : declaredFields) {
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Column){
					Column column = (Column)annotation;
					columnFieldMap.put(column, field);
					break;
				}
			}
		}
		return columnFieldMap;
	}
	
	private static void initFieldColumnMapping(List<Class<?>> clazzs){
		for (Class<?> clazz : clazzs) {
			FIELD_COLUMN_MAPPING.put(clazz, genFieldColumnMap(clazz));
			
		}
	}
	
	private static Map<Field, Column> genFieldColumnMap(Class<?> clazz){
		Map<Field, Column> columnFieldMap = new HashMap<>();
		Field[] declaredFields = clazz.getDeclaredFields();
		for (Field field : declaredFields) {
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Column){
					Column column = (Column)annotation;
					columnFieldMap.put(field, column);
					break;
				}
			}
		}
		return columnFieldMap;
	}
	
	private static void initFieldGetterMapping(List<Class<?>> clazzs){
		for (Class<?> clazz : clazzs) {
			FIELD_GETTER_MAPPING.put(clazz, genFieldGetterMap(clazz));
			
		}
	}
	
	private static Map<Field, Method> genFieldGetterMap(Class<?> clazz){
		Map<Field, Method> fieldMethodMap = new HashMap<>();
		Field[] declaredFields = clazz.getDeclaredFields();
		for (Field field : declaredFields) {
			try {
				PropertyDescriptor pd = new PropertyDescriptor(
						field.getName(), clazz);
				Method readMethod = pd.getReadMethod();
				fieldMethodMap.put(field, readMethod);
			} catch (IntrospectionException e) {
				String message = e.getMessage();
				// java.beans.IntrospectionException: Method not found: isAPir
				if(message.contains("Method not found:")){
					Method[] declaredMethods = clazz.getDeclaredMethods();
					String searchGetterName = message
							.replace("Method not found:", "").trim();
					boolean isFind = false;
					for (Method method : declaredMethods) {
						if(searchGetterName.equalsIgnoreCase(method.getName())){
							try {
								fieldMethodMap.put(field, method);
								isFind = true;
								break;
							} catch (IllegalArgumentException e1) {
								LOG.error("", e);
							}
						}
					}
					if(!isFind){
						searchGetterName = "get" + message
								.replace("Method not found:", "").trim().substring(2);
						for (Method method : declaredMethods) {
							if(searchGetterName.equalsIgnoreCase(method.getName())){
								try {
									fieldMethodMap.put(field, method);
									break;
								} catch (IllegalArgumentException e1) {
									LOG.error("", e);
								}
							}
						}
						
					}
				} else {
					LOG.error("", e);
					
				}
			}
		}
		return fieldMethodMap;
	}
	
	private static void initFieldSetterMapping(List<Class<?>> clazzs){
		for (Class<?> clazz : clazzs) {
			FIELD_SETTER_MAPPING.put(clazz, genFieldSetterMap(clazz));
			
		}
	}
	
	private static Map<Field, Method> genFieldSetterMap(Class<?> clazz){
		Map<Field, Method> fieldMethodMap = new HashMap<>();
		Field[] declaredFields = clazz.getDeclaredFields();
		for (Field field : declaredFields) {
			try {
				PropertyDescriptor pd = new PropertyDescriptor(
						field.getName(), clazz);
				Method readMethod = pd.getWriteMethod();
				fieldMethodMap.put(field, readMethod);
			} catch (IntrospectionException e) {
				String message = e.getMessage();
				// java.beans.IntrospectionException: Method not found: isAPir
				if(message.contains("Method not found:")){
					String searchGetterName = "set" + message
							.replace("Method not found:", "").trim().substring(2);
					Method[] declaredMethods = clazz.getDeclaredMethods();
					for (Method method : declaredMethods) {
						if(searchGetterName.equalsIgnoreCase(method.getName())){
							try {
								fieldMethodMap.put(field, method);
								break;
							} catch (IllegalArgumentException e1) {
								LOG.error("", e);
							}
						}
					}
				}
			}
		}
		return fieldMethodMap;
	}
	
	public static Map<String, Field> getColumnNameFieldMap(Class<?> clazz){
		Map<String, Field> result = new HashMap<>();
		Map<Column, Field> map = COLUMN_FIELD_MAPPING.get(clazz);
		for (Entry<Column,Field> entry : map.entrySet()) {
			Column column = entry.getKey();
			Field value = entry.getValue();
			result.put(column.name(), value);
		}
		return result;
	}
	
	
	private static void initColumnFieldIdMapping(List<Class<?>> clazzs){
		for (Class<?> clazz : clazzs) {
			COLUMN_FIELD_ID_MAPPING.put(clazz, genColumnFieldIdMap(clazz));
			
		}
	}
	
	private static Map<Column, Field> genColumnFieldIdMap(Class<?> clazz){
		Map<Column, Field> columnFieldIdMap = new HashMap<>();
		Field[] declaredFields = clazz.getDeclaredFields();
		
		for (Field field : declaredFields) {
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			boolean isId = false;
			Column column = null;
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Column){
					column = (Column)annotation;
				}
				if(annotation instanceof Id){
					isId = true;
				}
			}
			if(isId && column != null){
				columnFieldIdMap.put(column, field);
			}
		}
		return columnFieldIdMap;
	}
	

	private static void initFieldColumnIdMapping(List<Class<?>> clazzs){
		for (Class<?> clazz : clazzs) {
			FIELD_COLUMN_ID_MAPPING.put(clazz, genFieldColumnIdMap(clazz));
			
		}
	}
	
	private static Map<Field, Column> genFieldColumnIdMap(Class<?> clazz){
		Map<Field, Column> fieldColumnIdMap = new HashMap<>();
		Field[] declaredFields = clazz.getDeclaredFields();
		
		for (Field field : declaredFields) {
			Annotation[] declaredAnnotations = field.getDeclaredAnnotations();
			boolean isId = false;
			Column column = null;
			for (Annotation annotation : declaredAnnotations) {
				if(annotation instanceof Column){
					column = (Column)annotation;
				}
				if(annotation instanceof Id){
					isId = true;
				}
			}
			if(isId && column != null){
				fieldColumnIdMap.put(field, column);
			}
		}
		return fieldColumnIdMap;
	}
}
