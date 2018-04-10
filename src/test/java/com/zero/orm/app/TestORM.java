package com.zero.orm.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Date;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.logicalcobwebs.proxool.configuration.PropertyConfigurator;

import com.zero.orm.app.pojo.Stock;
import com.zero.orm.app.storage.StockStorage;

public class TestORM {

	@Before
	public void init() {// 把properties文件加载到链接对象
		try {
			InputStream is = new FileInputStream(new File(System.getProperty("user.dir") + "/conf/proxool.properties"));
			Properties properties = new Properties();
			properties.load(is);
			PropertyConfigurator.configure(properties);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testQuery() {
		StockStorage stockStorage = new StockStorage();
		Stock stock = new Stock();
		stock.setCode("XZ000001");
		stock.setName("零的世界");
		stock.setStatus(1);
		stock.setListDate(new Date(new java.util.Date().getTime()));
		stock.setIndustry("金融");
		stock.setBussiness("割韭菜");
		stock.setIsStock("Y");
		stock.setLocation("C");
		System.out.println(stock.getQuerySql());
		System.out.println(stock.getSaveSql());
		System.out.println(stock.getUpdateSql());
		System.out.println(stock.getUpdateSqlById());
		System.out.println(stock.getDeleteSql());
		System.out.println(stock.getDeleteByIdSql());
		System.out.println(stock.getParamList());
		System.out.println(stock.getUpdateByIdList());
		System.out.println(stock.getDeleteByIdList());
		System.out.println(stock.getExistSql());
//		String sql = stock.getQuerySql() + " WHERE CODE = ?";
//		for (int i = 0; i < 10; i++) {
//			stockStorage.query(sql, new Object[]{"sh600000"});
//			
//		}
	}

}
