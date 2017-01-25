package edu.iastate.geol.meteor.swat.bug.DAO;

import edu.iastate.geol.meteor.swat.bug.bean.Bug;

public interface BugDAO {

	public void setDataSource();
	
	public boolean insertBug(Bug bug);
	
}
