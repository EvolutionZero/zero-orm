package com.zero.orm.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Date;
import java.util.LinkedList;
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
		stock.setId(1024);
		stock.setCode("XZ000001");
		stock.setName("零的世界");
		stock.setStatus(1);
		stock.setListDate(new Date(new java.util.Date().getTime()));
		stock.setIndustry("金融");
		stock.setBussiness("割韭菜");
		stock.setIsStock("Y");
		stock.setLocation("C");
		stock.setPlate("深圳A股");
//		System.out.println(stock.getQuerySql());
		System.out.println(stock.getSaveSql());
		System.out.println(stock.getParamList());
//		System.out.println(stock.getUpdateSql());
//		System.out.println(stock.getUpdateSqlById());
//		System.out.println(stock.getDeleteSql());
//		System.out.println(stock.getDeleteByIdSql());
//		System.out.println(stock.getUpdateByIdList());
//		System.out.println(stock.getDeleteByIdList());
//		System.out.println(stock.getExistSql());
//		System.out.println(stock.getUpdateSqlByUniqueConstraints());
//		System.out.println(stock.getUpdateByUniqueConstraintsList());
		
		Stock stock2 = new Stock();
		stock2.setId(1024);
		stock2.setCode("XZ000002");
		stock2.setName("卡西欧");
		stock2.setStatus(1);
		stock2.setListDate(new Date(new java.util.Date().getTime()));
		stock2.setIndustry("金融");
		stock2.setBussiness("割韭菜");
		stock2.setIsStock("Y");
		stock2.setLocation("C");
		stock2.setPlate("深圳B股");
		
		LinkedList<Stock> list = new LinkedList<Stock>();
		list.add(stock);
		list.add(stock2);
		
//		stockStorage.saveOrUpdate(stock2);
		
//		stockStorage.saveUnique(list);
//		
//		String sql = stock.getQuerySql() + " WHERE CODE = ?";
//		for (int i = 0; i < 10; i++) {
//			System.out.println(stockStorage.query(sql, new Object[]{"sh600000"}));
//			
//		}
	}

}
