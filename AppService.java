/**
 *
 */
package com.prismmicro.resultset.service;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.prismmicro.resultset.exception.DatabaseConnectionFailException;
import com.prismmicro.resultset.exception.Neo4jConnectionException;
import com.prismmicro.resultset.exception.ResourceAlreadyExistsException;
import com.prismmicro.resultset.exception.ResourcePersistanceException;
import com.prismmicro.resultset.exception.ResultsetResourceNotFoundException;
import com.prismmicro.resultset.model.App;
import com.prismmicro.resultset.model.RegisterFilterRequest;
import com.prismmicro.resultset.model.Resultset;
import com.prismmicro.resultset.model.ResultsetId;
import com.prismmicro.resultset.model.SuccessResponse;
import com.prismmicro.resultset.repository.AppRepository;
import com.prismmicro.resultset.repository.ResultsetRepository;
import com.prismmicro.resultset.utility.ConnectionFactory;
import com.prismmicro.resultset.utility.Constants;

@Service
public class AppService {

	@Autowired
	AppRepository appRepository;

	@Autowired
	ConnectionFactory connectionFactory;

	@Autowired
	ResultsetRepository resultsetRepository;

	@Autowired
	ResultsetService resultsetService;

	@Autowired
	FilterService filterService;

	private static final Logger logger = LogManager.getLogger(AppService.class);

	public SuccessResponse registerApp(App app) {

		try {
			final Optional<App> existingApp = appRepository.findById(app.getAppId());
			if (existingApp.isPresent()) {
				throw new ResourceAlreadyExistsException("App id already exists");
			}
			if (app.getDbType().equals(Constants.DB_TYPE_POSTGRES)) {
				connectionFactory.testConnection(app.getConnectionString());
			} else if (app.getDbType().equals(Constants.DB_TYPE_IMPALA)) {
				connectionFactory.connectImpala(app.getConnectionString());
			} else if (app.getDbType().equals(Constants.DB_TYPE_NEO4J)) {
				connectionFactory.testConnection(app.getConnectionString(), app.getUsername(), app.getPassword());

			}
			appRepository.save(app);
			registerTablesAndColumns(app);
			return new SuccessResponse("App registered successfully");
		} catch (final DatabaseConnectionFailException e) {
			logger.error(e);
			throw new ResourcePersistanceException(
					"Unable to register App, please verify connection string, unable to connect to the database : "
							+ app.getConnectionString(),
					e);
		} catch (ResourcePersistanceException | ResourceAlreadyExistsException e) {
			logger.error(e);
			throw e;
		} catch (final Neo4jConnectionException e) {
			logger.error(e);
			throw new Neo4jConnectionException(Constants.ERROR_FAILED_TO_CONNECT_TO_NEO4JDB);
		} catch (final Exception e) {
			logger.error(e);
			throw new ResourcePersistanceException("unable to register App ", e);
		}

	}

	private void registerTablesAndColumns(App app) {
		try {
			// registering tables list for app
			resultsetService.registerTable(getResultset(app));
			// registering column list for app
			resultsetService.registerTable(getResultsetWithTablecolumns(app));
			// registering table filter
			filterService.registerFilter(getFilter(app));

		} catch (ResultsetResourceNotFoundException | ResourcePersistanceException | ResourceAlreadyExistsException e) {
			logger.error(e);
			throw e;
		}
	}

	private Resultset getResultsetWithTablecolumns(App app) {
		final Resultset resultset = new Resultset();
		resultset.setId(new ResultsetId(Constants.COLUMNS_DATA, app.getAppId()));
		resultset.setDescription("Registering columns list for app :" + app.getAppId());
		resultset.setEnableCRUD(true);
		resultset.setResultsetType(Constants.RESULTSET_TYPE_CUSTOM);
		if (app.getDbType().equals(Constants.DB_TYPE_IMPALA)) {
			resultset.setName(Constants.DB_TYPE_IMPALA);
			resultset.setQuery("describe ");
		} else if (app.getDbType().equals(Constants.DB_TYPE_NEO4J)) {
		}
		else {
			resultset.setName("information_schema.columns");
			resultset.setQuery("select column_name, data_type from  information_schema.columns ");
		}
		return resultset;
	}

	private RegisterFilterRequest getFilter(App app) {
		final RegisterFilterRequest filter = new RegisterFilterRequest();
		filter.setFilterId(Constants.TABLE_FILTER);
		filter.setAppId(app.getAppId());
		filter.setDatasource(Constants.TABLE_FILTER_DATA);
		filter.setDatasourceType("resultset");
		filter.setDescription("Filter for tables in " + app.getAppId());
		filter.setName("Table");
		filter.setFieldname("table");
		filter.setType("singleAutoComplete");
		filter.setWidth(4);
		filter.setColor("#8962ab");
		filter.setDefaultValue("NOT ASSIGNED");
		return filter;

	}

	private Resultset getResultset(App app) {
		final Resultset resultset = new Resultset();
		resultset.setId(new ResultsetId(Constants.TABLE_FILTER_DATA, app.getAppId()));
		resultset.setDescription("Registering table list for app :" + app.getAppId());
		resultset.setEnableCRUD(true);
		resultset.setResultsetType(Constants.RESULTSET_TYPE_FILTERQUERY);
		if (app.getDbType().equals(Constants.DB_TYPE_IMPALA)) {
			resultset.setQuery("SHOW TABLES like '*{q}*'");
		} else {
			resultset.setQuery("select table_name as value, table_name as label from  information_schema.tables "
					+ " WHERE table_schema='public' AND table_type='BASE TABLE' and table_name like '%{q}%'");
		}
		return resultset;
	}

	public SuccessResponse saveApp(App app) {

		try {
			final Optional<App> existingApp = appRepository.findById(app.getAppId());
			if (!existingApp.isPresent()) {
				throw new ResultsetResourceNotFoundException(
						"No record found with appId: " + app.getAppId() + " to update");
			}
			if (app.getDbType().equals(Constants.DB_TYPE_POSTGRES)) {
				connectionFactory.testConnection(app.getConnectionString());
			} 
			else if (app.getDbType().equals(Constants.DB_TYPE_IMPALA)){
				connectionFactory.connectImpala(app.getConnectionString());
			}
			else if(app.getDbType().equals(Constants.DB_TYPE_NEO4J))
			{
				connectionFactory.testConnection(app.getConnectionString(), app.getUsername(), app.getPassword());
			}
			
			appRepository.save(app);
			return new SuccessResponse("App saved successfully");

		} catch (final ResultsetResourceNotFoundException e) {
			logger.error(e);
			throw e;
		} catch (final DatabaseConnectionFailException e) {
			logger.error(e);
			throw new ResourcePersistanceException(
					"Unable to save App, please verify connection string, unable to connect to the database provided in the connection string ",
					e);
		} 
		  catch (final Neo4jConnectionException e) {
			logger.error(e);
			throw new Neo4jConnectionException(Constants.ERROR_FAILED_TO_CONNECT_TO_NEO4JDB);
		}catch (final Exception e) {
			logger.error(e);
			throw new ResourcePersistanceException("unable to save App ", e);
		}

	}

	public App getApp(String appId) {
		try {
			final Optional<App> optApp = appRepository.findById(appId);
			if (!optApp.isPresent()) {
				throw new ResultsetResourceNotFoundException("No record found with appId: " + appId);
			}
			return optApp.get();

		} catch (final ResultsetResourceNotFoundException e) {
			logger.error(e);
			throw e;
		} catch (final Exception e) {
			logger.error(e);
			throw new ResourcePersistanceException("unable to get App ", e);
		}
	}
}
