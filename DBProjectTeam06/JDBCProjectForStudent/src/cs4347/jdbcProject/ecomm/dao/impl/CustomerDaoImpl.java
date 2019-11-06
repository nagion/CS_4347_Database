package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cs4347.jdbcProject.ecomm.dao.CustomerDAO;
import cs4347.jdbcProject.ecomm.entity.Customer;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class CustomerDaoImpl implements CustomerDAO
{

	@Override
	public Customer create(Connection connection, Customer customer) throws SQLException, DAOException {
		//check if id is valid
		if(customer.getId() != null) 
		{
			throw new DAOException("invalid ID");
		} 
		else {
			//SQL statement
			String s1 = String.format("INSERT INTO customer (id, firstName, lastName, gender, dob, email) values(NULL, '%s', '%s', '%s', '%s', '%s');", customer.getFirstName(),customer.getLastName(), customer.getGender(), customer.getDob(), customer.getEmail());
			PreparedStatement db = connection.prepareStatement(s1, Statement.RETURN_GENERATED_KEYS);
			db.executeUpdate();
			
			//Copy the generated auto-increment primary key to the ID.
			ResultSet keys = db.getGeneratedKeys();
			keys.next();
			long id = keys.getLong(1);
			customer.setId(id);
			return customer;
		}
	}

	@Override
	public Customer retrieve(Connection connection, Long id) throws SQLException, DAOException {
		Statement db = connection.createStatement();
		//if id = null throw error
		if (id == null) 
		{
			throw new DAOException("Invalid ID, retrieve failed");
		}
		//query to get row for given id
		String s1 = "SELECT * FROM Customer where id = "+ id; 
		ResultSet rs = db.executeQuery(s1);
		if (!rs.next()) {
			return null;
		} else {
			//create new instance of Customer
			Customer myCustomer = new Customer();		
			myCustomer.setEmail(rs.getString("email"));
			myCustomer.setDob(rs.getDate("dob"));
			myCustomer.setId(rs.getLong("id"));
			myCustomer.setGender(rs.getString("gender").charAt(0));
			myCustomer.setFirstName(rs.getString("firstName"));
			myCustomer.setLastName(rs.getString("lastName"));
			return myCustomer;
		}
	}

	@Override
	public int update(Connection connection, Customer customer) throws SQLException, DAOException {
		Statement db = connection.createStatement();
		//if id = null throw error
		if (customer.getId() == null) 
		{
			throw new DAOException("Invalid ID, update failed");
		}
		
		String s1 = String.format("UPDATE Customer SET firstName = '%s', lastName = '%s', dob = '%s', gender = '%c', email = '%s' WHERE id = %d;", customer.getFirstName(), customer.getLastName(), customer.getDob(), customer.getGender(), customer.getEmail(), customer.getId());		
		int rows = db.executeUpdate(s1);
		return rows;
	}

	@Override
	public int delete(Connection connection, Long id) throws SQLException, DAOException {
		Statement db = connection.createStatement();
		//if id = null throw error
		if (id == null) 
		{
			throw new DAOException("Invalid ID, Delete failed");
		}
		//delete the corresponding customer
		String s1 = "DELETE FROM Customer where id= "+ id;
		int rows = db.executeUpdate(s1);
		return rows;
	}

	@Override
	public List<Customer> retrieveByZipCode(Connection connection, String zipCode) throws SQLException, DAOException {
		Statement db = connection.createStatement();
		String s1 = "Select * from Customer Left Join Address on Customer.id=Address.customerId where zipcode = '"  + zipCode + "'";

		ResultSet rs = db.executeQuery(s1);
		//create new instance of arraylist
		ArrayList<Customer> myCustomer = new ArrayList<Customer>();
		
		if (rs == null){
			throw new DAOException ("");
		}
		int index = 0;
		while (rs.next()) {
			//create new instance of Customer
			myCustomer.add(new Customer());
			myCustomer.get(index).setId(rs.getLong("id"));
			myCustomer.get(index).setFirstName(rs.getString("firstName"));
			myCustomer.get(index).setLastName(rs.getString("lastName"));
			myCustomer.get(index).setEmail(rs.getString("email"));
			myCustomer.get(index).setGender(rs.getString("gender").charAt(0));
			myCustomer.get(index).setDob(rs.getDate("dob"));
			index++;
		}
		return myCustomer;
	}

	@Override
	public List<Customer> retrieveByDOB(Connection connection, Date startDate, Date endDate)
			throws SQLException, DAOException {
		Statement db = connection.createStatement();
		String s1 = "Select * from Customer where dob >= '" + startDate + "' and dob < '"+ endDate +"';";
		ResultSet rs = db.executeQuery(s1);

		//create new instance of arraylist
		ArrayList<Customer> myCustomer = new ArrayList<Customer>();
		int index = 0;
		while (rs.next()) {
			//create new instance of Customer
			myCustomer.add(new Customer());
			myCustomer.get(index).setId(rs.getLong("id"));
			myCustomer.get(index).setFirstName(rs.getString("firstName"));
			myCustomer.get(index).setLastName(rs.getString("lastName"));
			myCustomer.get(index).setEmail(rs.getString("email"));
			myCustomer.get(index).setGender(rs.getString("gender").charAt(0));
			myCustomer.get(index).setDob(rs.getDate("dob"));
			index++;
		}
		return myCustomer;
	}
	
}
