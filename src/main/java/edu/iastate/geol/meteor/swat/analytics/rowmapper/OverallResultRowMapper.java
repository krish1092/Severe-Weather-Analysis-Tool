package edu.iastate.geol.meteor.swat.analytics.rowmapper;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import edu.iastate.geol.meteor.swat.analytics.bean.OverallResult;



public class OverallResultRowMapper implements RowMapper<OverallResult>{
	
	@Override
	public OverallResult mapRow(ResultSet rs, int row) throws SQLException {
		
		OverallResult overallResult = new OverallResult(); 
		overallResult.setClassification(rs.getString("classification"));
		overallResult.setClassificationCount(rs.getLong("classification_count"));
		overallResult.setEF0(rs.getLong("EF0"));
		overallResult.setEF1(rs.getLong("EF1"));
		overallResult.setEF2(rs.getLong("EF2"));
		overallResult.setEF3(rs.getLong("EF3"));
		overallResult.setEF4(rs.getLong("EF4"));
		overallResult.setEF5(rs.getLong("EF5"));
		
		overallResult.setFlashfloodCount(rs.getLong("flashflood_count"));
		
		overallResult.setHailBelow1(rs.getLong("hail < 1"));
		overallResult.setHailAbove1(rs.getLong("1 <= hail < 2"));
		overallResult.setHailAbove2(rs.getLong("hail >= 2"));
		
		overallResult.setThunderstormWindBelow65(rs.getLong("thunderstorm_wind < 65"));
		overallResult.setThunderstormWindAbove65(rs.getLong("thunderstorm_wind >= 65"));
		
		overallResult.setNullClassificationCount(rs.getLong("null_classification_count"));
	
		return overallResult;
	}

}
