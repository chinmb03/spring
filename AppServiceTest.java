package com.prismmicro.resultset.service;
/**
 * @author chinmaya
 * @CreatedOn June 25, 2018 
 */

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.prismmicro.resultset.exception.ResourceAlreadyExistsException;
import com.prismmicro.resultset.exception.ResourcePersistanceException;
import com.prismmicro.resultset.exception.ResultsetResourceNotFoundException;
import com.prismmicro.resultset.model.App;
import com.prismmicro.resultset.model.SuccessResponse;
import com.prismmicro.resultset.repository.AppRepository;
import com.prismmicro.resultset.repository.ResultsetRepository;
import com.prismmicro.resultset.utility.ConnectionFactory;

public class AppServiceTest {
	@InjectMocks
	AppService AppServiceTest ;
	@Mock
	AppRepository appRepository;
	@Mock
	ConnectionFactory connectionFactory;
	@Mock
	ResultsetRepository resultsetRepository;
	@Mock
	ResultsetService resultsetService;
	@Mock
	FilterService filterService;
	@Mock
    SuccessResponse successResponse ;
	@Before
	public void initMocks() {
		MockitoAnnotations.initMocks(this);
	}
	
	//============================== RegisterApp ==========================
	@Test(expected=ResourcePersistanceException.class)
	public void registerAppTest1() throws Exception {
		App app =new App();
		app.setAppId("appIdTest");
		SuccessResponse expected=AppServiceTest.registerApp(app);
		SuccessResponse actual=new SuccessResponse("App registered successfully");
		//verify
		assertEquals(actual, expected);
	}
	
	
	@Test
	public void registerAppTest3() throws Exception {
		App app =new App();
		app.setAppId("appIdTest");
		app.setDbType("dbType");
		when(appRepository.findById(app.getAppId())).thenReturn(Optional.empty());
		//when(app.getDbType().equals(Constants.DB_TYPE_IMPALA)).thenReturn(false);
		SuccessResponse expected=AppServiceTest.registerApp(app);
		SuccessResponse actual=new SuccessResponse("App registered successfully");
		//verify
		assertEquals(actual, expected);
	}
	
	
	@Test
	public void registerAppTest4() throws Exception {
		App app =new App();
		String url="url";
		app.setAppId("appIdTest");
		app.setDbType("Impala");
		app.setConnectionString("connectionString");
		when(appRepository.findById(app.getAppId())).thenReturn(Optional.empty());
		//when(app.getDbType().equals(Constants.DB_TYPE_IMPALA)).thenReturn(false);
		SuccessResponse expected=AppServiceTest.registerApp(app);
		SuccessResponse actual=new SuccessResponse("App registered successfully");
		//verify
		assertEquals(actual, expected);
	}

	
	@Test(expected=ResourceAlreadyExistsException.class)
	public void registerAppTest2() throws Exception {
		App app =new App();
		app.setAppId("appIdTest");
		Optional<App> app1=Optional.of(app);
		when(appRepository.findById(app.getAppId())).thenReturn(app1);
		SuccessResponse actual=AppServiceTest.registerApp(app);
		
	}
	
	//============================== saveApp ==========================
	
	@Test
	public void saveAppTest() throws Exception {
		App app =new App();
		app.setAppId("appIdTest");
		app.setDbType("Impala");
		Optional<App> existingApp=Optional.of(app);
		when(appRepository.findById(app.getAppId())).thenReturn(existingApp);
		SuccessResponse actual=AppServiceTest.saveApp(app);
		SuccessResponse expected =new SuccessResponse("App saved successfully");
		//verify
		assertEquals(actual, expected);
		
		
	}
	
	@Test(expected=ResultsetResourceNotFoundException.class)
	public void saveAppTest2() throws Exception {
		App app =new App();
		app.setAppId("appIdTest");
		when(appRepository.findById(app.getAppId())).thenReturn(Optional.empty());
		SuccessResponse actual=AppServiceTest.saveApp(app);
		
		
		
	}
	
	//============================== getApp ==========================
	@Test
	public void getAppTest1() throws Exception {
		String appId="appId";
		App app=new App();
		Optional<App> optApp=Optional.of(app);
		when(appRepository.findById(appId)).thenReturn(optApp);
		App actual=AppServiceTest.getApp(appId);
		App expected=app;
		assertEquals(expected, actual);
		
	}
	@Test(expected=ResultsetResourceNotFoundException.class)
	public void getAppTest2() throws Exception {
		String appId="appId";
		App app=new App();
		//Optional<App> optApp=Optional.of(empty());
		when(appRepository.findById(appId)).thenReturn(Optional.empty());
		App actual=AppServiceTest.getApp(appId);
	}
	
}

