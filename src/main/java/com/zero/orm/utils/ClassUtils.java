package com.zero.orm.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ClassUtils {

	private static final Logger LOG = LoggerFactory.getLogger(ClassUtils.class);
	
	public static String searchJarPath(Class<?> clazz){
		URL location = clazz.getProtectionDomain().getCodeSource().getLocation();
		if(location == null){
			return null;
		}
		try {
			return location.toURI().getPath();
		} catch (URISyntaxException e) {
			LOG.error("", e);
		}
		return null;
	}
	
	public static List<Class<?>> findSubClass(Class<?> father){
		List<Class<?>> result = new LinkedList<Class<?>>();
		List<Class<?>> allClazz = getAllClass();
		for (Class<?> clazz : allClazz) {
			if(clazz != father && father.isAssignableFrom(clazz)){
				result.add(clazz);
			}
		}
		return result;
	}
	
	public static List<Class<?>> getAllClass(){
		List<Class<?>> clazzs = new LinkedList<Class<?>>();
		ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
		try {
			Enumeration<URL> resources = contextClassLoader.getResources("./");
			List<URI> uris = new LinkedList<URI>();
			while(resources.hasMoreElements()){
				URL resource = resources.nextElement();
			    URI uri = resource.toURI();  
			    uris.add(uri);
			}
			for (URI uri : uris) {
				String path = new File(uri).getPath();
				if(path.contains("jar")){
					clazzs = loadFromJar(path);
				} else {
					clazzs = loadFromDir(path, path);
				}
			}
		} catch (IOException e) {
			LOG.error("", e);
		} catch (URISyntaxException e) {
			LOG.error("", e);
		}
		return clazzs;
	}
	
	public static List<Class<?>> loadFromDir(String rootDirPath, String dirPath){
		List<Class<?>> clazzs = new LinkedList<Class<?>>();
		File dirFile = new File(dirPath);
		File[] files = dirFile.listFiles();
		if(files == null){
			return clazzs;
		}
		for (File file : files) {
			if(file.isDirectory()){
				clazzs.addAll(loadFromDir(rootDirPath, file.getAbsolutePath()));
				
			} else if(file.isFile() && file.getAbsolutePath().endsWith(".class")){
				String clazzName = file.getAbsolutePath().replace("\\", "/").replace(rootDirPath.replace("\\", "/"), "")
						.replace("/", ".").substring(1).replace(".class", "");
				try {
					clazzs.add(Thread.currentThread().getContextClassLoader().loadClass(clazzName));
				} catch (ClassNotFoundException e) {
					LOG.error("", e);
				}
			} else if(file.isFile() && file.getAbsolutePath().endsWith(".jar")){
				clazzs.addAll(loadFromJar(file.getAbsolutePath()));
			}
		}
		return clazzs;
	}
	
	
	public static List<Class<?>> loadFromJar(String jarPath){
		List<Class<?>> clazzs = new LinkedList<Class<?>>();
		String realJarPath = jarPath;
		if(jarPath.contains("!")){
			realJarPath = jarPath.substring(0, jarPath.indexOf("!"));
		}
		JarFile jarFile = null; 
		String clazzName = null;
        try {
        	jarFile = new JarFile(new File(realJarPath));  
        	Enumeration<JarEntry> entries = jarFile.entries();
        	while(entries.hasMoreElements()){
        		JarEntry jarEntry = entries.nextElement();
        		if(jarEntry.getName().endsWith(".class")){
        			clazzName = jarEntry.getName().replace("/", ".").replace(".class", "");
					clazzs.add(Thread.currentThread().getContextClassLoader().loadClass(clazzName));
				}
        	}
		} catch (FileNotFoundException e) {
			LOG.error("", e);
		} catch (IOException e) {
			LOG.error("", e);
		} catch (ClassNotFoundException e) {
			LOG.error("", e);
		} catch(NoClassDefFoundError e){
			LOG.error("", e);
		} catch(IncompatibleClassChangeError e){
			LOG.error("", e);
			
		}  finally {
			 if (jarFile != null) {    
	                try {    
	                	jarFile.close();    
	                } catch (IOException e) {    
	                	LOG.error("", e);
	                }    
	            }  
		}
        return clazzs;
	}
	
}
