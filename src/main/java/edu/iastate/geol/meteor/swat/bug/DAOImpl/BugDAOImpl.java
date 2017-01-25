package edu.iastate.geol.meteor.swat.bug.DAOImpl;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import edu.iastate.geol.meteor.swat.bug.DAO.BugDAO;
import edu.iastate.geol.meteor.swat.bug.bean.Bug;

public class BugDAOImpl implements BugDAO {

	private JdbcTemplate jdbcTemplate;
	
	@Override
	public void setDataSource()
	{

		
		SimpleDriverDataSource dataSource = new SimpleDriverDataSource();
        try{
        	dataSource.setDriver(new com.mysql.jdbc.Driver());
        }catch(Exception e)
        {
        	
        }
        dataSource.setUrl("jdbc:mysql://localhost:3306/swat");
        dataSource.setUsername("root");
        dataSource.setPassword("SwatTool@2015");
        //dataSource.setPassword("Swat@2016");
		
		this.jdbcTemplate = new JdbcTemplate(dataSource);
	

	}

	@Override
	public boolean insertBug(Bug bug) 
	{
		String sql = "insert into bugs values(?,?)";
		try
		{
			jdbcTemplate.update(sql, null, bug.getDescription());
			return true;
		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
		
	}


}
