package com.zero.orm.core;

import java.util.List;

public interface IPojo {

	public abstract String getQuerySql();

	public abstract String getSaveSql();

	public abstract String getUpdateSql();

	public abstract String getUpdateSqlById();

	public abstract String getDeleteSql();

	public abstract String getDeleteByIdSql();

	public abstract List<Object> getParamList();

	public abstract Object[] getParamArray();

	public abstract List<Object> getUpdateByIdList();

	public abstract Object[] getUpdateByIdArray();

	public abstract List<Object> getDeleteByIdList();

	public abstract Object[] getDeleteByIdArray();

}