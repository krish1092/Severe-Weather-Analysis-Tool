package edu.iastate.geol.meteor.swat.analytics.rowmapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;

import edu.iastate.geol.meteor.swat.analytics.bean.AnalyticsClassificationBean;

public class AnalyticsClassificationBeanResultSetExtractor implements ResultSetExtractor<List<AnalyticsClassificationBean>> {

	@Override
	public List<AnalyticsClassificationBean> extractData(ResultSet rs) throws SQLException, DataAccessException {
		
		List<AnalyticsClassificationBean> list = new ArrayList<AnalyticsClassificationBean>();
		AnalyticsClassificationBean aCB ;
		while(rs.next())
		{
		aCB = new AnalyticsClassificationBean();
		aCB.setEmailAddress(rs.getString("email_Address"));
		aCB.setClassification(rs.getString("classification"));
		list.add(aCB);
		}
		return list;
	}

}
