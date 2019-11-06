package cs4347.jdbcProject.ecomm.dao.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import cs4347.jdbcProject.ecomm.dao.AddressDAO;
import cs4347.jdbcProject.ecomm.entity.Address;
import cs4347.jdbcProject.ecomm.util.DAOException;

public class AddressDaoImpl implements AddressDAO
{

	@Override
	
	public Address create(Connection connection, Address address, Long customerID) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		Statement statement = connection.createStatement();
		String query = String.format("Insert into address (address1, address2,  zipcode, city, state, customerID) values ('%s', '%s', '%s', '%s', '%s', %d);" , address.getAddress1(),address.getAddress2(), address.getZipcode(), address.getCity(), address.getState(), customerID);
		statement.executeUpdate(query);
		return address;
	}

	@Override
	public Address retrieveForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		// TODO Auto-generated method stub
	     Statement statement = connection.createStatement();
	     String query = String.format("select address1, address2, zipcode, city, state from simple_company.address where customerID = %d", customerID);
	     ResultSet res = statement.executeQuery(query);
	     
	     if(!res.next())
	     {
	    	 return null;
	     }
	     
	    Address result = new Address();
	 	result.setAddress1(res.getString("address1"));
		result.setAddress2(res.getString("address2"));
		result.setZipcode(res.getString("zipcode"));
		result.setCity(res.getString("city"));
		result.setState(res.getString("state"));
		
		return result;
	}

	@Override
	public void deleteForCustomerID(Connection connection, Long customerID) throws SQLException, DAOException {
		// TODO Auto-generated method stub
		Statement statement = connection.createStatement();
		String query = String.format("DELETE FROM simple_company.address where customerId = %d", customerID);
		statement.executeUpdate(query);
		
	}

}
