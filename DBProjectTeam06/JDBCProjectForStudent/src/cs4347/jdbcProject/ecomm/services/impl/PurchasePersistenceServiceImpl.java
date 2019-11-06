/*
	This part was done by AZEEM ALI (aaa096020)
*/

package cs4347.jdbcProject.ecomm.services.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.impl.PurchaseDaoImpl;
import cs4347.jdbcProject.ecomm.entity.Purchase;
import cs4347.jdbcProject.ecomm.services.PurchasePersistenceService;
import cs4347.jdbcProject.ecomm.services.PurchaseSummary;
import cs4347.jdbcProject.ecomm.util.DAOException;


public class PurchasePersistenceServiceImpl implements PurchasePersistenceService
{
	private DataSource dataSource;

	public PurchasePersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}

	@Override
	public Purchase create(Purchase purchase) throws SQLException, DAOException 
	{
		//create new instance
		PurchaseDaoImpl PurchaseImpl = new PurchaseDaoImpl();
		
		//get connection
		Connection connection = dataSource.getConnection();
		
		try 
		{
			//turn off autoCommit
			connection.setAutoCommit(false);
			
			Purchase p = PurchaseImpl.create(connection, purchase);
			
			Long purchId = p.getId();
			
			//throw error for null purchase id
			if(purchId == null) 
			{
				throw new DAOException("Error: Purchase ID is NULL");
			}

			//commit the changes
			connection.commit();
			return p;
		}
		catch (Exception e) 
		{
			//if exception, rollback the changes
			connection.rollback();
			throw e;
		}
		
		finally 
		{
			//close the connection
			if (connection != null) 
			{
				//turn autoCommit back on
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) 
			{
				connection.close();
			}
		}
	}

	@Override
	public Purchase retrieve(Long id) throws SQLException, DAOException 
	{
		//create new instance
		PurchaseDaoImpl PurchaseImpl = new PurchaseDaoImpl();
		
		//get connection
		Connection connection = dataSource.getConnection();
		
		try 
		{
			//turn autoCommit off
			connection.setAutoCommit(false);
			
			Purchase p = PurchaseImpl.retrieve(connection, id);

			//commit
			connection.commit();
			return p;
		}
		catch (Exception e) 
		{
			//if exception, rollback the changes
			connection.rollback();
			throw e;
		}
		
		finally 
		{
			//close the connection
			if (connection != null) 
			{
				//turn autoCommit back on
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) 
			{
				connection.close();
			}
		}
	}

	@Override
	public int update(Purchase purchase) throws SQLException, DAOException 
	{
		//create new instance
		PurchaseDaoImpl PurchaseImpl = new PurchaseDaoImpl();
		
		//get connection
		Connection connection = dataSource.getConnection();
		
		try 
		{
			//turn autoCommit off
			connection.setAutoCommit(false);
			
			int numRows = PurchaseImpl.update(connection, purchase);
			
			//commit
			connection.commit();
			return numRows;
		}
		catch (Exception e) 
		{
			//if exception, rollback the changes
			connection.rollback();
			throw e;
		}
		
		finally 
		{
			//close the connection
			if (connection != null) 
			{
				//turn autoCommit back on
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) 
			{
				connection.close();
			}
		}
	}

	@Override
	public int delete(Long id) throws SQLException, DAOException 
	{
		//create new instance
		PurchaseDaoImpl PurchaseImpl = new PurchaseDaoImpl();
		
		//get connection
		Connection connection = dataSource.getConnection();
		
		try 
		{
			//turn autoCommit of
			connection.setAutoCommit(false);
		
			// if the id is less than at least 1, it is invalid
			if (id < 1)
			{
				throw new DAOException("Invalid ID. Value less than 1. id = : " + id);
			}
			
			int numRows = PurchaseImpl.delete(connection, id);
			
			//if the count of rows is 0, something went wrong
			if (numRows == 0) 
			{
				throw new DAOException("Error: Delete Failed");
			}
			
			//commit
			connection.commit();
			return numRows;
		}
		catch (Exception e) 
		{
			//if exception, rollback the changes
			connection.rollback();
			throw e;
		}
		
		finally 
		{
			//close the connection
			if (connection != null) 
			{
				//turn autoCommit back on
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) 
			{
				connection.close();
			}
		}
	}

	@Override
	public List<Purchase> retrieveForCustomerID(Long customerID) throws SQLException, DAOException 
	{
		//create new instance
		PurchaseDaoImpl PurchaseImpl = new PurchaseDaoImpl();
		
		//get connection
		Connection connection = dataSource.getConnection();
		
		try 
		{
			//turn autoCommit off
			connection.setAutoCommit(false);
			
			List<Purchase> result = PurchaseImpl.retrieveForCustomerID(connection, customerID);
			
			//if nothing is returned
			if (result.size() == 0) 
			{
				throw new DAOException("An error occured in retrieveForCustomerID");
			}
			
			//commit
			connection.commit();
			return result;
		}
		catch (Exception e) 
		{
			//if exception, rollback the changes
			connection.rollback();
			throw e;
		}
		
		finally 
		{
			//close the connection
			if (connection != null) 
			{
				//turn autoCommit back on
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) 
			{
				connection.close();
			}
		}
	}

	@Override
	public PurchaseSummary retrievePurchaseSummary(Long customerID) throws SQLException, DAOException 
	{
		//create new instance
		PurchaseDaoImpl PurchaseImpl = new PurchaseDaoImpl();
		
		//get connection
		Connection connection = dataSource.getConnection();
		
		try 
		{
			//turn off autoCommit
			connection.setAutoCommit(false);
			
			//purchase summary
			PurchaseSummary psum = PurchaseImpl.retrievePurchaseSummary(connection, customerID);
			
			//commit
			connection.commit();
			return psum;
		}
		catch (Exception e) 
		{
			//if exception, rollback the changes
			connection.rollback();
			throw e;
		}
		
		finally 
		{
			//close the connection
			if (connection != null) 
			{
				//turn autoCommit back on
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) 
			{
				connection.close();
			}
		}
	}

	@Override
	public List<Purchase> retrieveForProductID(Long productID) throws SQLException, DAOException 
	{
		//create new instance
		PurchaseDaoImpl PurchaseImpl = new PurchaseDaoImpl();
		
		//get connection
		Connection connection = dataSource.getConnection();
		
		try 
		{
			//turn off autoCommit
			connection.setAutoCommit(false);
			
			List<Purchase> result = PurchaseImpl.retrieveForCustomerID(connection, productID);
			
			//if nothing is returned
			if (result.size() == 0) 
			{
				throw new DAOException("An error occured in retrieveForProductID");
			}
			
			//commit
			connection.commit();
			return result;
		}
		catch (Exception e) 
		{
			//if exception, rollback the changes
			connection.rollback();
			throw e;
		}
		
		finally 
		{
			//close the connection
			if (connection != null) 
			{
				//turn autoCommit back on
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) 
			{
				connection.close();
			}
		}
	}
}
