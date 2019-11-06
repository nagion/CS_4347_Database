package cs4347.jdbcProject.ecomm.services.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import cs4347.jdbcProject.ecomm.dao.AddressDAO;
import cs4347.jdbcProject.ecomm.dao.CreditCardDAO;
import cs4347.jdbcProject.ecomm.dao.CustomerDAO;
import cs4347.jdbcProject.ecomm.dao.impl.AddressDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.CreditCardDaoImpl;
import cs4347.jdbcProject.ecomm.dao.impl.CustomerDaoImpl;
import cs4347.jdbcProject.ecomm.entity.Address;
import cs4347.jdbcProject.ecomm.entity.CreditCard;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.services.CustomerPersistenceService;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class CustomerPersistenceServiceImpl implements CustomerPersistenceService
{
	private DataSource dataSource;

	public CustomerPersistenceServiceImpl(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}
	
	@Override
	public Customer create(Customer customer) throws SQLException, DAOException
	{
		//create new instance for customer, address, and creditcard
		CustomerDAO customerDAO = new CustomerDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

		Connection connection = dataSource.getConnection();
		try {
			//turn off auto commit
			connection.setAutoCommit(false);
			
			Customer custom = customerDAO.create(connection, customer);
			
			Long customID = custom.getId();
			//throw error for null id
			if (customID == null)
			{
				throw new DAOException("Customers ID is invalid");
			}
			
			//throw error for null address
			if (custom.getAddress() == null) {
				throw new DAOException("Customer's address is invalid");
			}
			Address address = custom.getAddress();
			addressDAO.create(connection, address, customID);
			
			//throw error for null creditcard
			if (custom.getCreditCard() == null) {
				throw new DAOException("Customer's credit card is invalid");
			}
			CreditCard creditCard = custom.getCreditCard();
			creditCardDAO.create(connection, creditCard, customID);

			//commit the change
			connection.commit();
			return custom;
		}
		catch (Exception ex) {
			//if exception caught, we roll back change
			connection.rollback();
			throw ex;
		}
		finally {
			//make sure connection closed
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	@Override
	public Customer retrieve(Long id) throws SQLException, DAOException {
		
		//create new instance for customer, address, and creditcard
		CustomerDAO customerDAO = new CustomerDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();

		Connection connection = dataSource.getConnection();
		try {
			//trun auto commit off
			connection.setAutoCommit(false);
			//make sure id is valid
			if (id <= 0) {
				throw new DAOException("invalid id bound");
			}
			Customer custom = customerDAO.retrieve(connection, id);
			Long customID = custom.getId();
			//throw exception if customer ID is null
			if (customID == null) {
				throw new DAOException("Customers ID is invalid");
			}
			//apply action
			Address address = addressDAO.retrieveForCustomerID(connection, customID);
			custom.setAddress(address);
			
			CreditCard creditCard = creditCardDAO.retrieveForCustomerID(connection, customID);
			custom.setCreditCard(creditCard);
			//commit the action
			connection.commit();
			return custom;
		}
		catch (Exception ex) {
			//roll back if exception is caught
			connection.rollback();
			throw ex;
		}
		finally {
			//make sure connection is closed
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	@Override
	public int update(Customer customer) throws SQLException, DAOException {
		//create new instance
		CustomerDAO customerDAO = new CustomerDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();
		
		Connection connection = dataSource.getConnection();
		try {
			//turn off auto commit
			connection.setAutoCommit(false);
			//check ID
			if (customer.getId() == null) {
				throw new DAOException("customer ID is invalid");
			}
			//check address
			if (customer.getAddress() == null) {
				throw new DAOException("customer address is invalid");
			}
			//check credit card
			if (customer.getCreditCard() == null) {
				throw new DAOException("customer credit card is invalid");
			}
			//apply action
			long customerID = customer.getId();
			addressDAO.deleteForCustomerID(connection, customerID);
			creditCardDAO.deleteForCustomerID(connection, customerID);
			
			addressDAO.create(connection, customer.getAddress(), customerID);
			creditCardDAO.create(connection, customer.getCreditCard(), customerID);
			int numRows = customerDAO.update(connection, customer);
			
			//commit the change
			connection.commit();
			return numRows;
		}
		catch (Exception ex) {
			//roll back change if exception is caught
			connection.rollback();
			throw ex;
		}
		finally {
			//make sure connection is closed
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	@Override
	public int delete(Long id) throws SQLException, DAOException {
		//create customer instance
		CustomerDAO customerDAO = new CustomerDaoImpl();

		Connection connection = dataSource.getConnection();
		try {
			//set auto commit off
			connection.setAutoCommit(false);
			//throw exception for invalid id
			if (id <= 0) {
				throw new DAOException("invalid id bound");
			}
			
			int rows = customerDAO.delete(connection, id);
			//commit change
			connection.commit();
			return rows;
		}
		catch (Exception ex) {
			//roll back change if exception is caught
			connection.rollback();
			throw ex;
		}
		finally {
			//make sure connection is closed
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}

	@Override
	public List<Customer> retrieveByZipCode(String zipCode) throws SQLException, DAOException {
		//Create new instance
		CustomerDAO customerDAO = new CustomerDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();
		
		Connection connection = dataSource.getConnection();
		try {
			//set auto commit off
			connection.setAutoCommit(false);
			
			//for every list, store the list by zipcode
			List<Customer> list = customerDAO.retrieveByZipCode(connection, zipCode);
			for (Customer custom : list) {
				CreditCard creditCard = creditCardDAO.retrieveForCustomerID(connection, custom.getId());
				Address address = addressDAO.retrieveForCustomerID(connection, custom.getId());
				custom.setAddress(address);
				custom.setCreditCard(creditCard);
			}
			//commit change
			connection.commit();
			return list;
		}
		catch (Exception ex) {
			//commit roll back if exception is caught
			connection.rollback();
			throw ex;
		}
		finally {
			//connection close
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
		
	}

	@Override
	public List<Customer> retrieveByDOB(Date startDate, Date endDate) throws SQLException, DAOException {
		//create new instance
		CustomerDAO customerDAO = new CustomerDaoImpl();
		AddressDAO addressDAO = new AddressDaoImpl();
		CreditCardDAO creditCardDAO = new CreditCardDaoImpl();
		
		Connection connection = dataSource.getConnection();
		try {
			//set auto commit off
			connection.setAutoCommit(false);
			
			//throw exception if date is null
			if (startDate ==null || endDate == null ) {
				throw new DAOException("Invalid dates");
			}
			//for every list, store the list by date of birth
			List<Customer> list = customerDAO.retrieveByDOB(connection, startDate, endDate);
			for (Customer cust : list) {
				CreditCard creditCard = creditCardDAO.retrieveForCustomerID(connection, cust.getId());
				Address address = addressDAO.retrieveForCustomerID(connection, cust.getId());
				cust.setAddress(address);
				cust.setCreditCard(creditCard);
			}
			//commit change
			connection.commit();
			return list;
		}
		catch (Exception ex) {
			//roll back change if exception is caught
			connection.rollback();
			throw ex;
		}
		finally {
			//close connection
			if (connection != null) {
				connection.setAutoCommit(true);
			}
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		}
	}
}
