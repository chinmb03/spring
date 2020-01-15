package com.prismmicro.resultset.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.prismmicro.resultset.exception.ResourceAlreadyExistsException;
import com.prismmicro.resultset.exception.ResultsetResourceNotFoundException;
import com.prismmicro.resultset.model.App;
import com.prismmicro.resultset.model.Filter;
import com.prismmicro.resultset.model.FilterConfiguration;
import com.prismmicro.resultset.model.FilterId;
import com.prismmicro.resultset.model.LoadFilterRequest;
import com.prismmicro.resultset.model.RegisterFilterRequest;
import com.prismmicro.resultset.model.ReturnFilterListRequest;
import com.prismmicro.resultset.model.SuccessResponse;
import com.prismmicro.resultset.repository.AppRepository;
import com.prismmicro.resultset.repository.FilterRepository;
import com.prismmicro.resultset.repository.UtilityRepository;

/**
 * @author chinmaya
 * @CreatedOn June 25, 2018 
 */


public class FilterServiceTest {
	@InjectMocks
	FilterService filterServiceTest ;
	@Mock
	AppRepository appRepository;
	@Mock
	FilterRepository filterRepository;
	@Mock
	UtilityRepository filterUtilityRepository;
	@Mock
    HttpServletRequest request;
	@Before
	public void initMocks() {
		MockitoAnnotations.initMocks(this);
	}
	
	//============================== GetFilterList ==========================
	
	@Test(expected=ResultsetResourceNotFoundException.class)
	public void getFilterListTest1() throws Exception {
		ReturnFilterListRequest filterRequest =new ReturnFilterListRequest();		
		filterRequest.setAppId("appIdTest");
		final JSONArray jsonArray = new JSONArray();
		List<String> filterlist =new ArrayList();
		filterlist.add("filterList");
		filterRequest.setFilterIds(filterlist);
		//when(filterRequest.getFilterIds()).thenReturn(filterlist);
		Filter filter=new Filter();
		when(filterRepository.findById(new FilterId(new App("appId"), "filterId"))).thenReturn(Optional.of(filter));
		String expected=filterServiceTest.getFilterList(filterRequest);
		
	
	}
	
	
	//============================== RegisterFilter ==========================
	@Test
	public void registerFilterTest1() throws Exception {
		RegisterFilterRequest reqFilter =new RegisterFilterRequest();
		reqFilter.setDatasource("datasource");
		App app= new App();
		
		
		when(appRepository.findById(reqFilter.getAppId())).thenReturn(Optional.of(app));
		SuccessResponse actual=filterServiceTest.registerFilter(reqFilter);
		SuccessResponse expected =new SuccessResponse("Filter registered successfully");
		//verify
		assertEquals(actual, expected);
		
	}
	
	@Test(expected=ResultsetResourceNotFoundException.class)
	public void registerFilterTest2() throws Exception {
		RegisterFilterRequest reqFilter =new RegisterFilterRequest();
		reqFilter.setDatasource("datasource");
		when(appRepository.findById(reqFilter.getAppId())).thenReturn(Optional.empty());
		SuccessResponse actual=filterServiceTest.registerFilter(reqFilter);
				
	}
	
	
	@Test(expected=ResourceAlreadyExistsException.class)
	public void registerFilterTest3() throws Exception {
		RegisterFilterRequest reqFilter =new RegisterFilterRequest();
		reqFilter.setDatasource("datasource");
		Filter filter=new Filter();
		App dummyApp=new App();
		Optional<App> app=Optional.of(dummyApp);
		when(appRepository.findById(reqFilter.getAppId())).thenReturn(Optional.of(dummyApp));
		when(filterRepository.findById(new FilterId(app.get(), reqFilter.getFilterId()))).thenReturn(Optional.of(filter));
		SuccessResponse actual=filterServiceTest.registerFilter(reqFilter);
				
	}

	
	//============================== SaveFilter ==========================
	
	@Test(expected=ResourceAlreadyExistsException.class)
	public void saveFilterTest1() throws Exception {
		RegisterFilterRequest reqFilter =new RegisterFilterRequest();
		reqFilter.setDatasource("datasource");
		reqFilter.setFilterId("filterId");
		App app= new App();
		Mockito.<Optional<App>>when(appRepository.findById(reqFilter.getAppId())).thenReturn(Optional.of(app));
		SuccessResponse actual=filterServiceTest.saveFilter(reqFilter);
	
	}
	
	@Test(expected=ResultsetResourceNotFoundException.class)
	public void saveFilterTest2() throws Exception {
		RegisterFilterRequest reqFilter =new RegisterFilterRequest();
		reqFilter.setDatasource("datasource");
		reqFilter.setFilterId("filterId");		
		Mockito.<Optional<App>>when(appRepository.findById(reqFilter.getAppId())).thenReturn(Optional.empty());
		SuccessResponse actual=filterServiceTest.saveFilter(reqFilter);
	
	}
	
	@Test
	public void saveFilterTest3() throws Exception {
		RegisterFilterRequest reqFilter =new RegisterFilterRequest();
		reqFilter.setDatasource("datasource");
		reqFilter.setFilterId("filterId");
		App app1= new App();
		Optional<App> app = Optional.of(app1);
		Filter filter =new Filter();
		Mockito.<Optional<Filter>>when(filterRepository
		.findById(new FilterId(app.get(), reqFilter.getFilterId()))).thenReturn(Optional.of(filter));
		Mockito.<Optional<App>>when(appRepository.findById(reqFilter.getAppId())).thenReturn(Optional.of(app1));
		SuccessResponse actual=filterServiceTest.saveFilter(reqFilter);
		SuccessResponse expected=new SuccessResponse("Filter saved successfully");
		//verify
		assertEquals(actual, expected);
	}
	
	
	@Test(expected=Exception.class)
	public void saveFiltertest4() throws Exception {
		RegisterFilterRequest reqFilter =new RegisterFilterRequest();
		reqFilter.setFilterId("filterId");
		App app1= new App();
		Optional<App> app = Optional.of(app1);
		Filter filter =new Filter();
	    when(filterRepository.save(filter)).thenReturn(filter);
		SuccessResponse actual=filterServiceTest.saveFilter(reqFilter);
		
	}
	//============================== LoadFilterDatar ==========================
	
	@Test(expected=ResultsetResourceNotFoundException.class)
	public void loadFilterDataTest1() throws Exception {
		final Map<String, Object> mapTest = new HashMap<String, Object>();
		mapTest.put("dummy", "test");
		List<Map<String,Object>> response =new ArrayList();
		response.add(mapTest);
		Filter filterTest=new Filter();
		Optional<Filter> filter = Optional.of(filterTest);
		String filterId="dummyfilter";
		App app=new App();
		app.setAppId("appId");
		LoadFilterRequest loadFilterRequest=new LoadFilterRequest();
		loadFilterRequest.setAppId("AppId");
		String appId=loadFilterRequest.getAppId();
		FilterConfiguration filterConfiguration=new FilterConfiguration();
		List<FilterConfiguration> filters=new ArrayList();
		filters.add(filterConfiguration);
		when(filterRepository.findById(new FilterId(new App(appId), filterId))).thenReturn(filter);
		List<Map<String, Object>> actual=filterServiceTest.loadFilterData("appId", "filterId", filters, "q");
		
	}
	/*@Test(expected=ResultsetResourceNotFoundException.class)
	public void loadFilterDataTest2() throws Exception {
		
		String appId="appId";
		String resultsetId="resultsetId";
		String customQuery="customQuery";
		String filterId="filterId";
		String q="dummy";
		Resultset resultset=new Resultset();
		FilterConfiguration filterConfiguration=new FilterConfiguration();
		List<FilterConfiguration> filters=new ArrayList();
		filters.add(filterConfiguration);
		TableRequest tableRequest=new TableRequest(resultsetId, appId, customQuery, filters);
		Filter filter=new Filter();
		Optional<Filter>  optFilter=Optional.of(filter);
		Map<String, Object> mapTest=new HashMap<>();
		mapTest.put("dummy", "test");
		when(filterRepository.findById(any())).thenReturn(optFilter);
		when( utilityRepository.getFilterData(any(),any())).thenReturn(mapTest);
		List<Map<String, Object>> actual=filterServiceTest.loadFilterData(appId, filterId, filters, q);
		
	}*/
/*	@Test
	public void loadFilterDataTest2() throws Exception {
		String appId="appId";
		String resultsetId="resultsetId";
		String customQuery="customQuery";
		String filterId="filterId";
		String q="dummy";
		Resultset resultset=new Resultset();
		FilterConfiguration filterConfiguration=new FilterConfiguration();
		List<FilterConfiguration> filters=new ArrayList();
		filters.add(filterConfiguration);
		FilterId id=new FilterId();
		Filter filter=new Filter();
		Optional<Filter> optfilter = Optional.of(filter);
		TableRequest request=new TableRequest(resultsetId, appId, customQuery, filters);
		App app=new App();
		app.setDbType("Imapala");
		Optional<App> optApp =Optional.of(app);
		Map<String, Object> map=new HashMap<>();
		map.put("key", "string");
		filter.setDatasource("datasource");
		filter.setId(id);
		when(filterRepository.findById(any())).thenReturn(optfilter);
		when(utilityRepository.getFilterData(
						any(),any())).thenReturn(map);
		when( appRepository.findById(appId)).thenReturn(optApp);
		//when(resultsetService.getTable(request.getResultsetId(), request.getAppId())).thenReturn(resultset);
		filterServiceTest.loadFilterData("appId", "filterId", filters, "q");
		
	}*/
	@Test(expected=ResultsetResourceNotFoundException.class)
	public void loadFilterDataTest3() throws Exception {
		String appId="appId";
		String resultsetId="resultsetId";
		String customQuery="customQuery";
		String filterId="filterId";
		Resultset resultset=new Resultset();
		FilterConfiguration filterConfiguration=new FilterConfiguration();
		List<FilterConfiguration> filters=new ArrayList();
		filters.add(filterConfiguration);
		FilterId id=new FilterId();
		Filter filter=new Filter();
		DataSourceTableDataPage tableDataPage=new DataSourceTableDataPage();
		tableDataPage.setTableName("tableName");
		Optional<Filter> optfilter = Optional.of(filter);
		TableRequest request=new TableRequest(resultsetId, appId, customQuery, filters);
		App app=new App();
		app.setDbType("Imapala");
		Optional<App> optApp =Optional.of(app);
		Map<String, Object> map=new HashMap<>();
		map.put("key", "string");
		filter.setDatasource("datasource");
		filter.setId(id);
		when(filterRepository.findById(any())).thenReturn(optfilter);
		when(utilityRepository.getFilterData(any(),any())).thenReturn(map);
		when( appRepository.findById(appId)).thenReturn(optApp);
		//when(utilityRepository.getDbType(appId).equals(Constants.DB_TYPE_IMPALA)).thenReturn(true);
		//when(resultsetService.getTable(request.getResultsetId(), request.getAppId())).thenReturn(resultset);
		filterServiceTest.loadFilterData("appId", "filterId", filters, "q");
		
	}

	
	//============================== returnFilter ==========================
	@Test
	public void returnFilterTest1() throws Exception {
		String appId="appId";
		String filterId="filterId";
		App app=new App();
		app.setAppId(appId);
		//FilterId filterid= new FilterId(app,filterId);
		FilterId  filterid = new FilterId(new App(appId), filterId);
		Filter filter=new Filter();
		filter.setDescription("description");
		filter.setType("type");
		filter.setWidth(1);
		filter.setName("name");
		filter.setId(filterid);
		filter.setColor("color");
		filter.setLabelFieldName("labelFieldName");
		filter.setDataSourceType("resultset");
		app.setAppId("appId");
		
		
		
		Optional<Filter> optfilter=Optional.of(filter);
		when(filterRepository.findById(filterid)).thenReturn(optfilter);
		JSONObject actual=filterServiceTest.returnFilter(appId, filterId);
		System.out.println(actual);
		assertTrue(true);
		}
	@Test
	public void returnFilterTest2() throws Exception {
		String appId="appId";
		String filterId="filterId";
		App app=new App();
		app.setAppId(appId);
		//FilterId filterid= new FilterId(app,filterId);
		FilterId  filterid = new FilterId(new App(appId), filterId);
		Filter filter=new Filter();
		filter.setDescription("description");
		filter.setType("type");
		filter.setWidth(1);
		filter.setName("name");
		filter.setId(filterid);
		filter.setColor("color");
		filter.setLabelFieldName("labelFieldName");
		filter.setDataSourceType("data");
		filter.setDatasource("datasource");
		filter.setDefaultValue("defaultValue");
		app.setAppId("appId");
	
		Optional<Filter> optfilter=Optional.of(filter);
		when(filterRepository.findById(filterid)).thenReturn(optfilter);
		JSONObject actual=filterServiceTest.returnFilter(appId, filterId);
		assertTrue(true);
		}
}
