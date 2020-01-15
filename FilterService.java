/**
 *
 */
package com.prismmicro.resultset.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.collections4.map.HashedMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.prismmicro.resultset.exception.DatabaseConnectionFailException;
import com.prismmicro.resultset.exception.JSONParserException;
import com.prismmicro.resultset.exception.PostgresQueryException;
import com.prismmicro.resultset.exception.ResourceAlreadyExistsException;
import com.prismmicro.resultset.exception.ResourcePersistanceException;
import com.prismmicro.resultset.exception.ResultsetResourceNotFoundException;
import com.prismmicro.resultset.model.App;
import com.prismmicro.resultset.model.DataSourceTableDataPage;
import com.prismmicro.resultset.model.Filter;
import com.prismmicro.resultset.model.FilterConfiguration;
import com.prismmicro.resultset.model.FilterId;
import com.prismmicro.resultset.model.RegisterFilterRequest;
import com.prismmicro.resultset.model.ReturnFilterListRequest;
import com.prismmicro.resultset.model.SuccessResponse;
import com.prismmicro.resultset.model.TableRequest;
import com.prismmicro.resultset.repository.AppRepository;
import com.prismmicro.resultset.repository.FilterRepository;
import com.prismmicro.resultset.repository.UtilityRepository;
import com.prismmicro.resultset.utility.Constants;
import com.prismmicro.resultset.utility.ResultsetJsonParser;

/**
 * @author Geethasree
 *
 */
@Service("filterService")
public class FilterService {

	@Autowired
	AppRepository appRepository;

	@Autowired
	FilterRepository filterRepository;

	@Autowired
	UtilityRepository filterUtilityRepository;

	@Autowired
	Environment env;

	private static final Logger logger = LogManager.getLogger(FilterService.class);

	/** returns filter List data based on appiId and filterIds */
	public String getFilterList(ReturnFilterListRequest filterRequest) {
		final JSONArray jsonArray = new JSONArray();
		filterRequest.getFilterIds()
				.forEach(filterId -> jsonArray.put(returnFilter(filterRequest.getAppId(), filterId)));
		return jsonArray.toString();
	}

	/** returns filter data based on appId,filterId and q (custom query) */
	public List<Map<String, Object>> loadFilterData(String appId, String filterId, List<FilterConfiguration> filters,
			String q) {

		final Optional<Filter> rawFilter = filterRepository.findById(new FilterId(new App(appId), filterId));
		if (!rawFilter.isPresent()) {
			throw new ResultsetResourceNotFoundException(
					"No record found with provided id's appId: " + appId + Constants.FILTER_ID + filterId);
		} else {
			
			final Filter filter = rawFilter.get();
			try {

				final Map<String, Object> rawResponse = filterUtilityRepository.getFilterData(
						new TableRequest(filter.getDatasource(), filter.getId().getApp().getAppId(), q, filters),
						filter);

				final DataSourceTableDataPage tableDataPage = (DataSourceTableDataPage) rawResponse.get("data");
				return getData(tableDataPage, filterId, appId);
			} catch (final DatabaseConnectionFailException | PostgresQueryException | ResourceAlreadyExistsException
					| ResultsetResourceNotFoundException e) {
				logger.error(e);
				throw e;
			} catch (final Exception e) {
				logger.error(e);
				throw new ResultsetResourceNotFoundException("No mapped record found with provided id's appId: " + appId
						+ Constants.FILTER_ID + filterId + " in result set table");
			}

		}
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getData(DataSourceTableDataPage tableDataPage, String filterId, String appId) {
		List<Map<String, Object>> response = null;
		if ("table_filter".equalsIgnoreCase(filterId)
				&& filterUtilityRepository.getDbType(appId).equals(Constants.DB_TYPE_IMPALA)) {
			response = processResponse(tableDataPage);
		} else {
			response = (List<Map<String, Object>>) tableDataPage.getContent();
		}
		return response;
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> processResponse(DataSourceTableDataPage tableDataPage) {

		List<Map<String, Object>> rawData = (List<Map<String, Object>>) tableDataPage.getContent();
		List<Map<String, Object>> response = new ArrayList<>();
		rawData.forEach(data -> {
			Map<String, Object> dataRes = new HashedMap<>();
			dataRes.put("value", data.get("name"));
			dataRes.put("label", data.get("name"));
			response.add(dataRes);
		});
		return response;
	}

	/** return filter object based on appid and filterid */
	public JSONObject returnFilter(String appId, String filterId) {
		Filter filter = null;
		final JSONObject jsonObject = new JSONObject();
		final Optional<Filter> rawFilter = filterRepository.findById(new FilterId(new App(appId), filterId));
		if (!rawFilter.isPresent()) {
			throw new ResultsetResourceNotFoundException(
					"No record found with provided id's appId: " + appId + Constants.FILTER_ID + filterId);
		} else {
			try {
				filter = rawFilter.get();
				jsonObject.put("description", filter.getDescription());
				jsonObject.put("type", filter.getType());
				jsonObject.put("width", filter.getWidth());
				jsonObject.put("displayName", filter.getName());
				jsonObject.put("fieldName", filter.getFieldName());
				jsonObject.put("appId", filter.getId().getApp().getAppId());
				jsonObject.put("filterId", filter.getId().getFilterId());
				jsonObject.put("color", filter.getColor());
				jsonObject.put("labelFieldName", filter.getLabelFieldName());
				jsonObject.put("combination", filter.isCombination());
				if (filter.getDataSourceType().equalsIgnoreCase("data")) {

					if (filter.getDatasource() != null && !filter.getDatasource().isEmpty()) {
						jsonObject.put("data", convertToJSON(filter.getDatasource()));
					}
					if (filter.getDefaultValue() != null && !filter.getDefaultValue().isEmpty()) {
						jsonObject.put("defaultValue", convertToJSON(filter.getDefaultValue()));
					}

				} else if (filter.getDataSourceType().equalsIgnoreCase("resultset")) {

					jsonObject.put("data", "/loadFilter");
					jsonObject.put("defaultValue", filter.getDefaultValue());
					jsonObject.put("resultsetId", filter.getDatasource());

				}
			} catch (final JSONException e) {
				logger.error(e);
				throw new JSONParserException("JSON Parser exception, while converting data", e);
			}
			return jsonObject;
		}

	}

	public SuccessResponse registerFilter(RegisterFilterRequest reqFilter) {

		try {
			/** validate json */
			convertToJSON(reqFilter.getDatasource());

			final Optional<App> app = appRepository.findById(reqFilter.getAppId());
			if (!app.isPresent()) {
				throw new ResultsetResourceNotFoundException("No record found with appId: " + reqFilter.getAppId());
			}
			final Optional<Filter> existingFilter = filterRepository
					.findById(new FilterId(app.get(), reqFilter.getFilterId()));
			if (existingFilter.isPresent()) {
				throw new ResourceAlreadyExistsException("filter id already exists");
			}
			filterRepository.save(createFilterObject(reqFilter, app.get()));
			return new SuccessResponse("Filter registered successfully");
		} catch (final ResourceAlreadyExistsException | ResultsetResourceNotFoundException e) {
			logger.error(e);
			throw e;
		} catch (final JSONException e) {
			logger.error(e);
			throw new JSONParserException("Please provide valid JSON for datasource", e);
		} catch (final Exception e) {
			logger.error(e);
			throw new ResourcePersistanceException("Unable to register filter ", e);
		}
	}

	private Object convertToJSON(String data) throws JSONException {
		return ResultsetJsonParser.getObjectFromString(data);
	}

	public SuccessResponse saveFilter(RegisterFilterRequest reqFilter) {

		try {
			/** validate json */
			convertToJSON(reqFilter.getDatasource());
			final Optional<App> app = appRepository.findById(reqFilter.getAppId());
			if (!app.isPresent()) {
				throw new ResultsetResourceNotFoundException("No record found with appId: " + reqFilter.getAppId());
			}
			final Optional<Filter> existingFilter = filterRepository
					.findById(new FilterId(app.get(), reqFilter.getFilterId()));
			if (!existingFilter.isPresent()) {
				throw new ResourceAlreadyExistsException("no record found with filter id :" + reqFilter.getFilterId());
			}

			filterRepository.save(createFilterObject(reqFilter, app.get()));
			return new SuccessResponse("Filter saved successfully");
		} catch (final ResourceAlreadyExistsException | ResultsetResourceNotFoundException e) {
			logger.error(e);
			throw e;
		} catch (final JSONException e) {
			logger.error(e);
			throw new JSONParserException("Please provide valid JSON for datasource", e);
		} catch (final Exception e) {
			logger.error(e);
			throw new ResourcePersistanceException("Unable to save filter ", e);
		}
	}

	private Filter createFilterObject(RegisterFilterRequest reqFilter, App app) {
		final Filter filter = new Filter();
		filter.setColor(reqFilter.getColor());
		filter.setDatasource(reqFilter.getDatasource());
		filter.setDataSourceType(reqFilter.getDatasourceType());
		filter.setDefaultValue(reqFilter.getDefaultValue());
		filter.setDescription(reqFilter.getDescription());
		filter.setFieldName(reqFilter.getFieldname());
		filter.setId(new FilterId(app, reqFilter.getFilterId()));
		filter.setName(reqFilter.getName());
		filter.setType(reqFilter.getType());
		filter.setWidth(reqFilter.getWidth());
		filter.setLabelFieldName(reqFilter.getLabelFieldName());
		filter.setCombination(reqFilter.isCombination());
		filter.setCreatedBy(reqFilter.getCreatedBy());
		filter.setModifiedBy(reqFilter.getModifiedBy());
		return filter;
	}

}
