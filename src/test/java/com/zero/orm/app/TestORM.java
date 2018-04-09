package com.zero.orm.app;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
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
		String sql = new Stock().getQuerySql() + " WHERE CODE = ?";
		List<Stock> stocks = stockStorage.query(sql, new Object[]{"sh600000"});
		System.out.println(stocks);
	}

}
