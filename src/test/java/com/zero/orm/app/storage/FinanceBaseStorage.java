package com.zero.orm.app.storage;

import java.sql.Connection;

import com.zero.orm.core.BaseStorage;
import com.zero.orm.core.PojoBaseBean;

public class FinanceBaseStorage<T extends PojoBaseBean> extends BaseStorage<T>{

	@Override
	public Connection getConnection() {
		return null;
	}

}
