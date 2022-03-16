/** *****************************************************************************
 * MGDB BrAPI Impl - Mongo Genotype DataBase, BrAPI service implementation
 * Copyright (C) 2018, <CIRAD> <IRD>
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License, version 3 as published by
 * the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * See <http://www.gnu.org/licenses/agpl.html> for details about GNU General
 * Public License V3.
 ******************************************************************************
 */
package fr.cirad.web.controller.rest;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URLDecoder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.ejb.ObjectNotFoundException;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.map.UnmodifiableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.text.CaseUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.context.SecurityContextRepository;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.ServletContextAware;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

import fr.cirad.io.brapi.BrapiService;
import fr.cirad.mgdb.exporting.IExportHandler;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingProject;
import fr.cirad.mgdb.model.mongo.maintypes.GenotypingSample;
import fr.cirad.mgdb.model.mongo.maintypes.Individual;
import fr.cirad.mgdb.model.mongo.maintypes.VariantData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData;
import fr.cirad.mgdb.model.mongo.maintypes.VariantRunData.VariantRunDataId;
import fr.cirad.mgdb.model.mongo.subtypes.ReferencePosition;
import fr.cirad.mgdb.model.mongo.subtypes.SampleGenotype;
import fr.cirad.mgdb.model.mongodao.MgdbDao;
import fr.cirad.tools.AlphaNumericComparator;
import fr.cirad.tools.AppConfig;
import fr.cirad.tools.Helper;
import fr.cirad.tools.ProgressIndicator;
import fr.cirad.tools.mongo.MongoTemplateManager;
import fr.cirad.tools.security.base.AbstractTokenManager;
import fr.cirad.web.controller.BackOfficeController;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import jhi.brapi.api.Metadata;
import jhi.brapi.api.Pagination;
import jhi.brapi.api.Status;
import jhi.brapi.api.germplasm.BrapiGermplasm;

/**
 *
 * @author sempere
 *
 */
//@Api(tags = {"BrAPI"}, description = "BrAPI compliant methods")
@RestController
public class BrapiRestController implements ServletContextAware {

    /**
     * logger
     */
    static private final org.apache.log4j.Logger LOG = org.apache.log4j.Logger.getLogger(BrapiRestController.class);

    /**
     * The Constant TMP_OUTPUT_FOLDER.
     */
    static final private String TMP_OUTPUT_FOLDER = "genofilt/brapiTmpOutput";

    private ServletContext servletContext;

    static final int MAX_SUPPORTED_MARKER_LIST_SIZE = 200000;

    @Autowired
    private AbstractTokenManager tokenManager;
    @Autowired
    private SecurityContextRepository repository;
    @Autowired
    private AppConfig appConfig;

    @Autowired
    @Qualifier("authenticationManager")
    AuthenticationManager authenticationManager;

    /**
     * The Constant EXPORT_FILE_EXPIRATION_DELAY_MILLIS.
     */
    static final private long EXPORT_FILE_EXPIRATION_DELAY_MILLIS = 1000 * 60 * 60 * 24;
    /* 1 day */

    static public final String URL_BASE_PREFIX = "/brapi/v1";

    static public final String URL_CALLS = "calls";

    static public final String URL_LOGIN_V1_3 = "login";
    static public final String URL_LOGOUT_V1_3 = "logout";
    static public final String URL_GERMPLASM_SEARCH_V1_3 = "search/germplasm";
    static public final String URL_GERMPLASM_SEARCH_RESULT_V1_3 = "search/germplasm/{searchResultsDbId}";
    static public final String URL_GERMPLASM_SEARCH_GET_V1_3 = "germplasm";
    static public final String URL_STUDIES_V1_3 = "studies";

    /* calls without a version number specified are by default v1.1 */
    static public final String URL_TOKEN = "token";
    static public final String URL_MAPS = "maps";
    static public final String URL_MARKERS_SEARCH = "markers-search";
    static public final String URL_MARKER_DETAILS = "markers/{markerDbId}";
    static public final String URL_MAP_DETAILS = URL_MAPS + "/{mapDbId}";
    static public final String URL_MAP_POSITIONS = URL_MAPS + "/{mapDbId}/positions";
    static public final String URL_STUDY_GERMPLASMS = "studies/{studyDbId}/germplasm";
    static public final String URL_STUDIES = "studies-search";
    static public final String URL_MARKER_PROFILES = "markerprofiles";
    static public final String URL_ALLELE_MATRIX = "allelematrix-search";
    static public final String URL_ALLELE_MATRIX_STATUS = "allelematrix-search/status";
    static public final String URL_GERMPLASM_DETAILS = "germplasm/{germplasmDbId}";
    static public final String URL_GERMPLASM_SEARCH = "germplasm-search";
    static public final String URL_GERMPLASM_ATTRIBUTES = "germplasm/{germplasmDbId}/attributes";

//	static public final String URL_STUDY_GERMPLASMS_V1_0 = "studies/{id}/germplasm";
//	static public final String URL_MAP_DETAILS_V1_0 = URL_MAPS + "/{id}";
//	static public final String URL_MAP_POSITIONS_V1_0 = URL_MAPS + "/{id}/positions";
    static public final String URL_MARKERS_SEARCH_V1_0 = "markers";

    static private final TreeSet<CallMap> implementedCalls = new TreeSet<>();
    protected HashMap<String /*module*/, HashMap<Integer /*marker index when sorted by id*/, Comparable /*marker id*/>> markerIndexByModuleMap = new HashMap<>();

    static public final List<String> extRefList = new ArrayList<>();

    static {
        extRefList.addAll(Arrays.asList(BrapiService.BRAPI_FIELD_germplasmExternalReferenceId, BrapiService.BRAPI_FIELD_germplasmExternalReferenceSource, BrapiService.BRAPI_FIELD_germplasmExternalReferenceType, BrapiService.BRAPI_FIELD_extGermplasmDbId));
    }

    static class CallMap extends HashMap<String, Object> implements Comparable<CallMap> {	// for auto-sorting by call URL

        @Override
        public int compareTo(CallMap otherCallMap) {
            return ((String) get("call")).compareTo((String) otherCallMap.get("call"));
        }
    }

    static {
        CallMap call = new CallMap();
        call.put("call", URL_TOKEN);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"POST", "DELETE"});
        call.put("versions", new String[]{"1.1"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_LOGIN_V1_3);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"POST"});
        call.put("versions", new String[]{"1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_LOGOUT_V1_3);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"DELETE"});
        call.put("versions", new String[]{"1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_CALLS);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1", "1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_GERMPLASM_SEARCH_V1_3);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"POST"});
        call.put("versions", new String[]{"1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_STUDIES);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_STUDIES_V1_3);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_MAPS);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1", "1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_MARKER_PROFILES);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1", "1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_ALLELE_MATRIX);
        call.put("dataTypes", Arrays.asList(new String[]{"text/tsv", "application/json"}));
        call.put("methods", new String[]{"POST", "GET"});
        call.put("versions", new String[]{"1.1"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_ALLELE_MATRIX_STATUS);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_MAP_DETAILS);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1", "1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_MAP_POSITIONS);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1", "1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_MARKERS_SEARCH);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"POST", "GET"});
        call.put("versions", new String[]{"1.1"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_MARKER_DETAILS);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_STUDY_GERMPLASMS);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1", "1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_GERMPLASM_DETAILS);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_GERMPLASM_SEARCH_GET_V1_3);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.3"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_GERMPLASM_SEARCH);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"POST"});
        call.put("versions", new String[]{"1.1"});
        implementedCalls.add(call);

        call = new CallMap();
        call.put("call", URL_GERMPLASM_ATTRIBUTES);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.1", "1.3"});
        implementedCalls.add(call);

//    	call = new CallMap();
//    	call.put("call", URL_MAP_DETAILS_V1_0);
//    	call.put("dataTypes", Arrays.asList(new String[] {"application/json"}));
//    	call.put("methods", new String[] {"GET"});
//    	call.put("versions", new String[] {"1.0"});
//    	implementedCalls.add(call);
        call = new CallMap();
        call.put("call", URL_MARKERS_SEARCH_V1_0);
        call.put("dataTypes", Arrays.asList(new String[]{"application/json"}));
        call.put("methods", new String[]{"GET"});
        call.put("versions", new String[]{"1.0"});
        implementedCalls.add(call);

//    	call = new CallMap();
//    	call.put("call", URL_MAP_POSITIONS_V1_0);
//    	call.put("dataTypes", Arrays.asList(new String[] {"application/json"}));
//    	call.put("methods", new String[] {"GET"});
//    	call.put("versions", new String[] {"1.0"});
//    	implementedCalls.add(call);
//    	call = new CallMap();
//    	call.put("call", URL_STUDY_GERMPLASMS_V1_0);
//    	call.put("dataTypes", Arrays.asList(new String[] {"application/json"}));
//    	call.put("methods", new String[] {"GET"});
//    	call.put("versions", new String[] {"1.0"});
//    	implementedCalls.add(call);
    }

    /**
     * Handle all exceptions.
     *
     * @param request the request
     * @param response the response
     * @param ex the ex
     * @return the model and view
     */
    @ExceptionHandler(Exception.class)
    public ModelAndView handleAllExceptions(HttpServletRequest request, HttpServletResponse response, Exception ex) {
        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        HashMap<String, String> map = new HashMap<String, String>();
        boolean fLooksLikeMaxPostSizeIssue = ex instanceof MissingServletRequestParameterException && "Required Collection parameter 'markerprofileDbId' is not present".equals(ex.getMessage());
        if (fLooksLikeMaxPostSizeIssue) {
            map.put("errorMsg", "Unable to handle request body, you might want to disable maxPostSize");
        } else {
            map.put("errorMsg", ExceptionUtils.getStackTrace(ex));
        }
        LOG.error("Error at URL " + request.getRequestURI() + (request.getQueryString() == null ? "" : ("?" + request.getQueryString())) + (fLooksLikeMaxPostSizeIssue ? " - " + map.get("errorMsg") : ""), ex);
        return new ModelAndView(new MappingJackson2JsonView(), UnmodifiableMap.decorate(map));
    }

    static protected Map<String, Object> getStandardResponse(int pageSize, Integer currentPage, long totalCount, Integer desiredPageSize, boolean fIncludeResult) {
        Map<String, Object> resultObject = new HashMap<>();
        if (fIncludeResult) {
            Map<String, Object> result = new HashMap<>();
            ArrayList<Map<String, Object>> data = new ArrayList<>();
            result.put("data", data);
            resultObject.put("result", result);
        }
        Pagination pagination = new Pagination(pageSize, currentPage == null ? 0 : currentPage, totalCount, desiredPageSize == null || desiredPageSize < 1 ? (int) totalCount : desiredPageSize);
        Metadata metadata = new Metadata();
        metadata.setPagination(pagination);
        resultObject.put("metadata", metadata);

        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "home")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> home(HttpServletResponse response, @PathVariable String database) {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }

        Map<String, Object> resultObject = new LinkedHashMap<String, Object>();
        resultObject.put("version", "1.1, 1.3");
        resultObject.put("provider", "Gigwa - Genotype Investigator for Genome-Wide Analyses");
        String taxon = MongoTemplateManager.getTaxonName(database);
        String species = MongoTemplateManager.getSpecies(database);
        String taxoDesc = (species != null ? "Species: " + species : "") + (taxon != null && !taxon.equals(species) ? (species != null ? " ; " : "") + "Taxon: " + taxon : "");
        resultObject.put("description", "Database: " + database + " ; " + (!taxoDesc.isEmpty() ? taxoDesc + " ; " : "") + Helper.estimDocCount(mongoTemplate, Individual.class) + " germplasms ; " + Helper.estimDocCount(mongoTemplate, VariantData.class) + " markers");
        resultObject.put("contact", "gigwa@cirad.fr");
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "calls")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_CALLS, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> calls(HttpServletResponse response, @PathVariable String database, @RequestParam(required = false) String datatype, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }

//    	LOG.debug("calls called");
        ArrayList<Map<String, Object>> data = new ArrayList<>();
        for (CallMap call : implementedCalls) {
            if (datatype == null || ((List<String>) call.get("dataTypes")).contains(datatype)) {
                call.put("datatypes", call.get("dataTypes")); // hack to remain compatible with v1.1
                data.add(call);
            }
        }

        Map<String, Object> resultObject = getStandardResponse(data.size(), page, implementedCalls.size(), pageSize, true);
        ((Map<String, Object>) resultObject.get("result")).put("data", data);
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "mapList")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_MAPS, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> mapList(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestParam(required = false, name = "species") String speciesId,
            @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) throws IOException {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);

        ArrayList<Map<String, Object>> data = new ArrayList<>();
        if (speciesId == null || speciesId.equals(MongoTemplateManager.getTaxonName(database)))
			try {
            if (tokenManager.canUserReadDB(tokenManager.readToken(request), database)) {
                Map<String, Object> map = new HashMap<>();
                map.put("mapDbId", database);
                map.put("species", MongoTemplateManager.getSpecies(database));
                map.put("name", database + " map");
                map.put("type", "Physical");
                map.put("unit", "Mb");
                map.put("markerCount", Helper.estimDocCount(mongoTemplate, VariantData.class));
                HashSet<String> distinctSequences = new HashSet<>();
                Iterator<GenotypingProject> projectIt = mongoTemplate.find(new Query(), GenotypingProject.class).iterator();
                while (projectIt.hasNext()) {
                    distinctSequences.addAll(projectIt.next().getSequences());
                }
                map.put("linkageGroupCount", distinctSequences.size());
                data.add(map);
            }
        } catch (ObjectNotFoundException e) {
            build404Response(response);
        }
        Map<String, Object> resultObject = getStandardResponse(data.size(), 0, data.size(), pageSize, true);
        ((Map<String, Object>) resultObject.get("result")).put("data", data);
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "mapDetails")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_MAP_DETAILS, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> mapDetails(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @PathVariable("mapDbId") String mapDbId, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) throws IOException {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(mapDbId);
//		LOG.debug("mapDetails called");

        if (!database.equals(mapDbId)) {
            build404Response(response);
            return null;
        }

        Map<String, Object> resultObject = getStandardResponse(0, 0, 0, 0, false);
        Map<String, Object> map = new HashMap<>();
        try {
            if (tokenManager.canUserReadDB(tokenManager.readToken(request), database)) {
                map.put("mapDbId", mapDbId);
                map.put("name", database + " map");
                map.put("type", "Physical");
                map.put("unit", "Mb");

                List<HashMap<String, Comparable>> linkageGroups = new ArrayList<>();
                HashSet<String> distinctSequences = new HashSet<>();
                Iterator<GenotypingProject> projectIt = mongoTemplate.find(new Query(), GenotypingProject.class).iterator();
                while (projectIt.hasNext()) {
                    distinctSequences.addAll(projectIt.next().getSequences());
                }
                List<Document> pipeline = new ArrayList<Document>();
                pipeline.add(new Document("$match", new Document(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE, new Document("$in", distinctSequences))));
                Document groupObject = new Document("_id", "$" + VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE);
                groupObject.put("count", new Document("$sum", 1));
                pipeline.add(new Document("$group", groupObject));
                MongoCursor<Document> it = mongoTemplate.getCollection(MongoTemplateManager.getMongoCollectionName(VariantData.class)).aggregate(pipeline).iterator();
                while (it.hasNext()) {
                    Document obj = (Document) it.next();
                    HashMap<String, Comparable> linkageGroup = new HashMap<>();
                    linkageGroup.put("linkageGroupName", obj.getString("_id"));
                    linkageGroup.put("markerCount", obj.getInteger("count"));
                    Query query = new Query(Criteria.where(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE).is(obj.getString("_id")));
                    query.with(Sort.by(Sort.Direction.DESC, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE)).limit(1);
                    query.fields().include(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE);
                    VariantData variant = mongoTemplate.findOne(query, VariantData.class);
                    linkageGroup.put("maxPosition", variant.getReferencePosition().getStartSite());
                    linkageGroups.add(linkageGroup);
                }
                if (!linkageGroups.isEmpty()) {
                    map.put("data", linkageGroups);
                }
                ((Map<String, Object>) resultObject).put("result", map);
            }
        } catch (ObjectNotFoundException e) {
            build404Response(response);
        }
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "mapMarkerPositions")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_MAP_POSITIONS, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> mapMarkerPositions(HttpServletRequest request, HttpServletResponse response, @PathVariable final String database, @PathVariable String mapDbId, @RequestParam(required = false, name = "linkageGroupName") Collection<String> linkageGroupNames, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) throws IOException {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(mapDbId);
        long before = System.currentTimeMillis();
        ArrayList<Map<String, Object>> data = new ArrayList<>();
        long nCount = 0;

        // hack for remaining compatible with v1.0
        if (linkageGroupNames == null) {
            String linkageGroupIDs = request.getParameter("linkageGroupId");
            if (linkageGroupIDs != null) {
                linkageGroupNames = Helper.split(linkageGroupIDs, ",");
            }
        }

        if (pageSize == null || pageSize > MAX_SUPPORTED_MARKER_LIST_SIZE) {
            pageSize = MAX_SUPPORTED_MARKER_LIST_SIZE;
        }
        if (page == null) {
            page = 0;
        }

        try {
            if (tokenManager.canUserReadDB(tokenManager.readToken(request), database)) {
                List<Criteria> crit = new ArrayList<>();
                if (linkageGroupNames != null) {
                    crit.add(Criteria.where(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE).in(linkageGroupNames));
                }

                Document marker = null;

                Document projectObject = new Document();
                projectObject.put(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE, 1);
                projectObject.put(VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE, 1);

                HashMap<Integer, Comparable> markerIndexMap = markerIndexByModuleMap.get(database);
                if (markerIndexMap == null) {
                    markerIndexMap = new HashMap<Integer, Comparable>();
                    markerIndexByModuleMap.put(database, markerIndexMap);
                }

                nCount = mongoTemplate.count(crit.size() == 0 ? new Query() : new Query(new Criteria().andOperator(crit.toArray(new Criteria[crit.size()]))), VariantData.class);

                // hack to avoid using skip which slows down the query
                Comparable<Integer> previousMarker = page == 0 ? null : markerIndexMap.get(page * pageSize - 1);
                if (previousMarker != null) {
                    crit.add(Criteria.where("_id").gt(previousMarker));
                }

                FindIterable<Document> iterable = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).find(crit.size() == 0 ? new Document() : new Query(new Criteria().andOperator(crit.toArray(new Criteria[crit.size()]))).getQueryObject()).projection(projectObject).sort(new Document("_id", 1));
                if (pageSize != null) {
                    iterable.limit(pageSize);
                    if (page != null && previousMarker == null) {
                        iterable.skip(page * pageSize);
                    }
                }

                MongoCursor<Document> dbCursor = iterable.iterator();
                while (dbCursor.hasNext()) {
                    marker = dbCursor.next();
                    Map<String, Object> variant = new HashMap<>();
                    variant.put("markerDbId", marker.get("_id").toString());
                    variant.put("markerName", variant.get("markerDbId").toString());
                    variant.put("linkageGroupName", Helper.readPossiblyNestedField(marker, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_SEQUENCE, "; "));
                    variant.put("location", Helper.readPossiblyNestedField(marker, VariantData.FIELDNAME_REFERENCE_POSITION + "." + ReferencePosition.FIELDNAME_START_SITE, "; "));
                    data.add(variant);
                }
                if (marker != null) {
                    if (previousMarker != null) {
                        markerIndexByModuleMap.get(database).remove(page * pageSize - 1);	// we shouldn't need that one again
                    }
                    final int newIndex = page * pageSize + data.size() - 1;
                    markerIndexMap.put(newIndex, (Comparable<Integer>) marker.get("_id"));
                    new Timer().schedule(new TimerTask() {	// we only keep it for a minute     
                        @Override
                        public void run() {
                            markerIndexByModuleMap.get(database).remove(newIndex);
                        }
                    }, 60000);
                }
            }
        } catch (ObjectNotFoundException e) {
            build404Response(response);
        }
        Map<String, Object> resultObject = getStandardResponse(data.size(), page, nCount, pageSize, true);
        ((Map<String, Object>) resultObject.get("result")).put("data", data);
        LOG.debug("mapMarkerPositions took " + (System.currentTimeMillis() - before) / 1000d + "s for " + data.size() + " markers");

        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "studySummaryList")
    @CrossOrigin
    @RequestMapping(value = {"/{database:.+}" + URL_BASE_PREFIX + "/" + URL_STUDIES, "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_STUDIES_V1_3}, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> studySummaryList(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestParam(required = false) String studyType,
            @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
//		LOG.debug("studySummaryList called");

        ArrayList<Map<String, Object>> data = new ArrayList<>();
        if (studyType == null || "genotype".equals(studyType)) {
            Iterator<GenotypingProject> projectIt = mongoTemplate.findAll(GenotypingProject.class).iterator();
            while (projectIt.hasNext()) {
                GenotypingProject gp = projectIt.next();
                if (!tokenManager.canUserReadProject(tokenManager.readToken(request), database, gp.getId())) {
                    continue;
                }

                Map<String, Object> study = new HashMap<>(), additionalInfo = new HashMap<>();
                study.put("studyDbId", "" + gp.getId());
                study.put("name", gp.getName());
                study.put("startDate", gp.getCreationDate());
                study.put("studyType", "genotype");
                study.put("additionalInfo", additionalInfo);
                additionalInfo.put("ploidy", String.valueOf(gp.getPloidyLevel()));
                if (gp.getDescription() != null) {
                    additionalInfo.put("description", gp.getDescription());
                }
                data.add(study);
            }
        }

        Map<String, Object> resultObject = getStandardResponse(1, 0, data.size(), pageSize, true);
        ((Map<String, Object>) resultObject.get("result")).put("data", data);
        return resultObject;
    }

    @ApiOperation(authorizations = {
        @Authorization(value = "AuthorizationToken")}, value = "studyGerplasmList")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_STUDY_GERMPLASMS, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> studyGerplasmList(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @PathVariable(value = "studyDbId") int studyDbId, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
//		LOG.debug("studyGerplasms called");

        ArrayList<Map<String, Object>> data = new ArrayList<>();

        GenotypingProject gp = mongoTemplate.findById(studyDbId, GenotypingProject.class);
        if (gp != null && tokenManager.canUserReadProject(tokenManager.readToken(request), database, gp.getId())) {
            for (String individual : MgdbDao.getProjectIndividuals(database, gp.getId())) {
                Map<String, Object> germplasm = new HashMap<>();
                germplasm.put(BrapiService.BRAPI_FIELD_germplasmDbId, individual);
                germplasm.put("germplasmName", individual);
                germplasm.put("studyDbId", studyDbId);
                data.add(germplasm);
            }
        }

        Map<String, Object> resultObject = getStandardResponse(1, 0, data.size(), pageSize, true);
        ((Map<String, Object>) resultObject.get("result")).put("data", data);
        return resultObject;
    }

    @org.springframework.data.mongodb.core.mapping.Document(collection = "brapiGermplasmSearches")
    @TypeAlias("BGS")
    static public class GermplasmSearchRequest {
        static private final String FIELDNAME_CREATION_DATE = "cd";
        @Id
        private ObjectId _id;
        @Field(value = FIELDNAME_CREATION_DATE)
        @Indexed(expireAfterSeconds = 60 * 5)
        public Date createdAt = new Date();
        public Collection<String> germplasmPUIs;
        public Collection<String> germplasmDbIds;
        public Collection<String> germplasmNames;
        public Collection<String> germplasmSpecies;
        public Collection<String> germplasmGenus;
        public Collection<String> accessionNumbers;
        public Integer pageSize;
        public Integer page;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "germplasmAttributes")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_GERMPLASM_ATTRIBUTES, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> germplasmAttributes(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @PathVariable(name = "germplasmDbId") String germplasmDbId, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) throws IOException, ObjectNotFoundException {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }

        String token = tokenManager.readToken(request);
        if (!tokenManager.canUserReadDB(token, database)) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return null;
        }

        Authentication authentication = tokenManager.getAuthenticationFromToken(token);
        HashMap<String, Object> result = new HashMap<>();
        Individual ind = MgdbDao.getInstance().loadIndividualsWithAllMetadata(database, authentication == null ? "anonymousUser" : authentication.getName(), null, Arrays.asList(germplasmDbId)).get(germplasmDbId);
        if (ind == null) {
            build404Response(response);
            return null;
        }

        ArrayList<Map<String, Object>> data = new ArrayList<>();
        for (String attributeDbId : ind.getAdditionalInfo().keySet()) {
            data.add(new HashMap<String, Object>() {
                {
                    put("attributeDbId", attributeDbId);
                    put("value", ind.getAdditionalInfo().get(attributeDbId));
                }
            });
        }

        result.put("data", data);
        result.put(BrapiService.BRAPI_FIELD_germplasmDbId, germplasmDbId);

        Map<String, Object> resultObject = getStandardResponse(0, 1, data.size(), 0, false);
        resultObject.put("result", result);
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "germplasmSearch")
    @CrossOrigin
    @RequestMapping(value = {"/{database:.+}" + URL_BASE_PREFIX + "/" + URL_GERMPLASM_SEARCH_V1_3, "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_GERMPLASM_SEARCH}, method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
    public Map<String, Object> germplasmSearch(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestBody GermplasmSearchRequest requestBody) throws ObjectNotFoundException {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
//		LOG.debug("germplasmSearch called");

        if (!tokenManager.canUserReadDB(tokenManager.readToken(request), database)) {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            return null;
        }

        mongoTemplate.insert(requestBody);

        Map<String, Object> resultObject = getStandardResponse(0, 0, 0, 0, false);
        resultObject.put("result", new HashMap<String, Object>() {
            {
                put("searchResultsDbId", requestBody._id.toString());
                put("searchResultDbId", requestBody._id.toString());
            }
        });
        LOG.debug("Germplasm search criteria saved as " + requestBody._id);
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "getGermplasmSearchResults")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_GERMPLASM_SEARCH_RESULT_V1_3, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> getGermplasmSearchResults(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @PathVariable("searchResultsDbId") String searchResultsDbId) throws ObjectNotFoundException, IOException {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        GermplasmSearchRequest requestBody = mongoTemplate.findById(searchResultsDbId, GermplasmSearchRequest.class);
        if (requestBody == null) {	// it either expired or never existed
            build404Response(response);
            return null;
        }

        return executeGermplasmSearch(request, response, database, requestBody, BrapiGermplasm.germplasmFields);
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "germplasmSearchGetV1_3")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_GERMPLASM_SEARCH_GET_V1_3, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> germplasmSearchGetV1_3(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestParam(required = false) String germplasmDbId, @RequestParam(required = false) String germplasmName, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) throws Exception {
        return germplasmSearch(request, response, database, null, germplasmDbId, germplasmName, pageSize, page);
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "germplasmSearch")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_GERMPLASM_SEARCH, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> germplasmSearch(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestParam(required = false) String germplasmPUI, @RequestParam(required = false) String germplasmDbId, @RequestParam(required = false) String germplasmName, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) throws ObjectNotFoundException, IOException {
        GermplasmSearchRequest requestBody = new GermplasmSearchRequest();
        if (germplasmPUI != null) {
            requestBody.germplasmPUIs = Arrays.asList(germplasmPUI);
        }
        if (germplasmDbId != null) {
            requestBody.germplasmDbIds = Arrays.asList(germplasmDbId);
        }
        if (germplasmName != null) {
            requestBody.germplasmNames = Arrays.asList(germplasmName);
        }
        requestBody.pageSize = pageSize;
        requestBody.page = page;

        return executeGermplasmSearch(request, response, database, requestBody, BrapiGermplasm.germplasmFields);
    }

    // This method does the actual data retrieval, it shall be called by methods mapped to various call versions
    public Map<String, Object> executeGermplasmSearch(HttpServletRequest request, HttpServletResponse response, String database, GermplasmSearchRequest requestBody, Map<String, String> brapiFields) throws ObjectNotFoundException, IOException {
        if (requestBody.germplasmPUIs != null || requestBody.germplasmGenus != null || requestBody.accessionNumbers != null) {
            return getStandardResponse(0, 0, 0, 0, false);	// those parameters are not supported at the moment
        }
        String dbSpecies = MongoTemplateManager.getSpecies(database);
        if (requestBody.germplasmSpecies != null && dbSpecies != null) {
            List<String> lcTrimmedTaxonList = requestBody.germplasmSpecies.stream().map(sp -> sp.toLowerCase().trim()).collect(Collectors.toList());
            if (!lcTrimmedTaxonList.contains(dbSpecies.toLowerCase().trim())) {
                return getStandardResponse(0, 0, 0, 0, false);
            }
        }

        ArrayList<Map<String, Object>> data = new ArrayList<>();
        TreeSet<String> indIDs = new TreeSet<String>(requestBody.germplasmDbIds == null ? new ArrayList<>() : requestBody.germplasmDbIds);
        if (requestBody.germplasmNames != null) {
            indIDs.addAll(requestBody.germplasmNames);
        }

        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        Query q = indIDs.size() > 0 ? new Query(Criteria.where("_id").in(indIDs)) : new Query();
        q.fields().include("_id");
        long count = mongoTemplate.count(q, Individual.class);

        if (requestBody.pageSize != null) {
            q.limit(requestBody.pageSize);
            if (requestBody.page != null) {
                q.skip(requestBody.page * requestBody.pageSize);
            }
        }
        List<String> indIDsForCurrentPage = mongoTemplate.find(q, Individual.class).stream().map(ind -> ind.getId()).collect(Collectors.toList());

        Authentication auth = tokenManager.getAuthenticationFromToken(tokenManager.readToken(request));
        String sCurrentUser = auth == null || "anonymousUser".equals(auth.getName()) ? "anonymousUser" : auth.getName();
        for (Individual ind : MgdbDao.getInstance().loadIndividualsWithAllMetadata(database, sCurrentUser, null, indIDsForCurrentPage).values()) {
            Map<String, Object> germplasm = new HashMap<>();
            germplasm.put(BrapiService.BRAPI_FIELD_germplasmDbId, ind.getId());
            germplasm.put("germplasmName", ind.getId());

            if (ind.getAdditionalInfo() != null) {
                for (String key : ind.getAdditionalInfo().keySet()) {
                    String lcKey = CaseUtils.toCamelCase(key, false, '_', '-', '.').toLowerCase();
                    if (brapiFields.containsKey(lcKey)) {
                        germplasm.put(brapiFields.get(lcKey), ind.getAdditionalInfo().get(key));
                    }
                    if (extRefList.contains(key)) {
                        germplasm.put(key, ind.getAdditionalInfo().get(key));
                    }
                }
            }

            data.add(germplasm);
        }

        Map<String, Object> resultObject = getStandardResponse((int) (requestBody.pageSize == null ? count : Math.min(requestBody.pageSize, count)), requestBody.pageSize == null || requestBody.page == null || requestBody.page < 1 ? 0 : requestBody.page, count, requestBody.pageSize, true);
        ((Map<String, Object>) resultObject.get("result")).put("data", data);
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "germplasmDetails")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_GERMPLASM_DETAILS, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> germplasmDetails(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @PathVariable(BrapiService.BRAPI_FIELD_germplasmExternalReferenceId) String germplasmDbId, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) throws IOException, ObjectNotFoundException {
        Map<String, Object> resultObject = getStandardResponse(0, 0, 0, 0, false);

        Map<String, Object> resultList = germplasmSearch(request, response, database, null, germplasmDbId, null, pageSize, page);
        HashMap<String, Object> result = (HashMap<String, Object>) resultList.get("result");
        ArrayList<Map<String, Object>> data = (ArrayList<Map<String, Object>>) result.get("data");
        if (data == null || data.size() == 0) {
            build404Response(response);
            return null;
        }

        resultObject.put("result", data.get(0));
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "markerProfiles")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_MARKER_PROFILES, method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> markerProfiles(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestParam(required = false, name = BrapiService.BRAPI_FIELD_germplasmExternalReferenceId) Collection<String> germplasmDbIds,
            @RequestParam(required = false, name = "studyDbId") Integer studyDbId,
            @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) throws Exception {
        /*TODO: implement pagination*/
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
//		LOG.debug("markerProfiles called");
        long before = System.currentTimeMillis();

        Collection<String> individuals;
        if (germplasmDbIds != null) {
            individuals = germplasmDbIds;
        } else {
            individuals = mongoTemplate.getCollection(mongoTemplate.getCollectionName(Individual.class)).distinct("_id", String.class).into(new ArrayList<>());
        }

        ArrayList<Map<String, Object>> data = new ArrayList<>(); // we use this structure to keep them sorted

        Query q = studyDbId == null ? new Query() : new Query(Criteria.where("_id").is(studyDbId));
        Iterator<GenotypingProject> projectIt = mongoTemplate.find(q, GenotypingProject.class).iterator();
        while (projectIt.hasNext()) {
            GenotypingProject gp = projectIt.next();
            if (!tokenManager.canUserReadProject(tokenManager.readToken(request), database, gp.getId())) {
                continue;
            }

            ArrayList<GenotypingSample> samplesForProject = MgdbDao.getSamplesForProject(database, gp.getId(), individuals);
            for (GenotypingSample sample : samplesForProject) {
                Map<String, Object> markerProfile = new HashMap<>();
                String germplasmId = sample.getIndividual();
                markerProfile.put("markerprofileDbId", sample.getId().toString());
                markerProfile.put("uniqueDisplayName", sample.getId().toString());
                markerProfile.put(BrapiService.BRAPI_FIELD_germplasmDbId, germplasmId);
                markerProfile.put("sampleDbId", "" + sample.getId());
                markerProfile.put("analysisMethod", gp.getTechnology());
                markerProfile.put("extractDbId", "");
//				long resultCount = mongoTemplate.count(new Query(new Criteria().andOperator(
//						Criteria.where("_id." + VariantRunDataId.FIELDNAME_PROJECT_ID).is(gp.getId()),
//						Criteria.where(VariantRunData.FIELDNAME_SAMPLEGENOTYPES + "." + sample.getId() + "." + SampleGenotype.FIELDNAME_GENOTYPECODE).exists(true))
//					), VariantRunData.class);
//				markerProfile.put("resultCount", resultCount); // takes too much time to respond on big databases
                data.add(markerProfile);
            }
        }

        Map<String, Object> resultObject = getStandardResponse(data.size(), 0, data.size(), data.size(), true);
        ((Map<String, Object>) resultObject.get("result")).put("data", data);
        LOG.debug("markerprofiles took " + (System.currentTimeMillis() - before) / 1000d + "s");
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "markers")
    @CrossOrigin
    @RequestMapping(value = {"/{database:.+}" + URL_BASE_PREFIX + "/" + URL_MARKERS_SEARCH, "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_MARKERS_SEARCH_V1_0}, method = {RequestMethod.GET}, produces = "application/json")
    public Map<String, Object> markers(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestParam(required = false) Collection<String> markerDbIds, @RequestParam(required = false) Collection<String> name, @RequestParam(required = false) String matchMethod, @RequestParam(required = false) String include, @RequestParam(required = false) String type, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) throws ObjectNotFoundException, Exception {
//    	LOG.debug("markers called");
        long before = System.currentTimeMillis();

        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }

        Map<String, Object> result = new HashMap<>();
        ArrayList<HashMap<String, Object>> data = new ArrayList<>();
        long count = 0;
        if (tokenManager.canUserReadDB(tokenManager.readToken(request), database)) {
            List<Criteria> crits = new ArrayList<Criteria>();
            boolean fIDsPassed = markerDbIds != null && markerDbIds.size() > 0;
            if (fIDsPassed) {
                name = markerDbIds;	// IDs have priority over names
            }
            if (name != null && name.size() > 0) {
                if (fIDsPassed || "exact".equals(matchMethod)) {
                    crits.add(Criteria.where("_id").in(name));
                } else if ("case_insensitive".equals(matchMethod)) {
                    crits.add(Criteria.where("_id").in(name.stream().map(idString -> Pattern.compile(idString, Pattern.CASE_INSENSITIVE)).collect(Collectors.toList())));
                } else if ("wildcard".equals(matchMethod)) {
                    crits.add(Criteria.where("_id").in(name.stream().map(idString -> Pattern.compile("^" + idString.replaceAll("\\*", ".*").replaceAll("%", ".*").replaceAll("\\?", ".") + "$")).collect(Collectors.toList())));
                } else // if (matchMethod != null && !"exact".equals(matchMethod))
                {
                    throw new Exception(matchMethod + " matchMethod not supported");
                }
            }

            if (type != null && type.trim().length() > 0) {
                crits.add(Criteria.where(VariantData.FIELDNAME_TYPE).is(type.trim()));
            }

            if (pageSize == null || pageSize > MAX_SUPPORTED_MARKER_LIST_SIZE) {
                pageSize = MAX_SUPPORTED_MARKER_LIST_SIZE;
            }

            Query q = crits.size() == 0 ? new Query() : new Query(new Criteria().andOperator(crits.toArray(new Criteria[crits.size()])));
            count = mongoTemplate.count(q, VariantData.class);

            Document projectObject = new Document(VariantData.FIELDNAME_KNOWN_ALLELES, 1);
            projectObject.put(VariantData.FIELDNAME_TYPE, 1);
            projectObject.put(VariantData.FIELDNAME_REFERENCE_POSITION, 1);
            if ("synonyms".equals(include)) {
                projectObject.put(VariantData.FIELDNAME_SYNONYMS, 1);
            }

            FindIterable<Document> iterable = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).find(q == null ? new Document() : q.getQueryObject()).projection(projectObject);
            if (pageSize != null) {
                iterable.limit(pageSize);
                if (page != null) {
                    iterable.skip(page * pageSize);
                }
            }

            MongoCursor<Document> dbCursor = iterable.iterator();
            while (dbCursor.hasNext()) {
                Document dbVariant = dbCursor.next();
                HashMap<String, Object> variantDTO = new HashMap<>();
                String dbId = (String) dbVariant.get("_id");
                variantDTO.put("markerDbId", dbId.toString());
                String markerType = (String) dbVariant.get(VariantData.FIELDNAME_TYPE);
                if (markerType != null) {
                    variantDTO.put("type", markerType);
                }
                variantDTO.put("refAlt", (List<String>) dbVariant.get(VariantData.FIELDNAME_KNOWN_ALLELES));
                String defaultDisplayName = MgdbDao.idLooksGenerated(dbId) ? null : dbId.toString();	// we don't invent names for ObjectIDs since we would not able to apply a filter on them 
                variantDTO.put("defaultDisplayName", defaultDisplayName);
                variantDTO.put("analysisMethods", null);
                HashSet<String> synonymsObject = new HashSet<String>();
                if ("synonyms".equals(include)) {
                    Document synonyms = (Document) dbVariant.get(VariantData.FIELDNAME_SYNONYMS);
                    for (String synType : synonyms.keySet()) {
                        for (String synForType : (List<String>) synonyms.get(synType)) {
                            synonymsObject.add(synForType.toString());
                        }
                    }
                    variantDTO.put("synonyms", synonymsObject);
                }
                data.add(variantDTO);
            }
        }

        Map<String, Object> resultObject = getStandardResponse((int) (pageSize == null ? count : Math.min(pageSize, count)), pageSize == null || page == null || page < 1 ? 0 : page, count, pageSize, true);
        result.put("data", data);
        resultObject.put("result", result);

        LOG.debug("markers took " + (System.currentTimeMillis() - before) / 1000d + "s for " + data.size() + " markers");
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "markerDetails")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_MARKER_DETAILS, method = {RequestMethod.GET}, produces = "application/json")
    public Map<String, Object> markerDetails(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @PathVariable("markerDbId") String markerDbId) throws ObjectNotFoundException, Exception {
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }

        Map<String, Object> resultObject = getStandardResponse(0, 0, 0, 0, false);
        if (tokenManager.canUserReadDB(tokenManager.readToken(request), database)) {
            VariantData variant = mongoTemplate.findById(markerDbId, VariantData.class);
            if (variant == null) {
                build404Response(response);
            } else {
                HashMap<String, Object> variantDTO = new HashMap<>();
                variantDTO.put("markerDbId", markerDbId);
                variantDTO.put("type", variant.getType());
                variantDTO.put("refAlt", variant.getKnownAlleles());
                String defaultDisplayName = MgdbDao.idLooksGenerated(markerDbId) ? null : markerDbId;	// we don't invent names for generated IDs since we would not able to apply a filter on them 
                variantDTO.put("defaultDisplayName", defaultDisplayName);
                variantDTO.put("analysisMethods", null);
                HashSet<String> synonymsObject = new HashSet<String>();
                if (variant.getSynonyms() != null) {
                    for (String synType : variant.getSynonyms().keySet()) {
                        for (String synForType : (Collection<String>) variant.getSynonyms().get(synType)) {
                            synonymsObject.add(synForType.toString());
                        }
                    }
                }
                variantDTO.put("synonyms", synonymsObject);
                resultObject.put("result", variantDTO);
            }
        }
        return resultObject;
    }

    static public class MarkerSearchRequest {
        public Collection<String> markerDbIds;
        public Collection<String> name;
        public String matchMethod;
        public String include;
        public String type;
        public Integer pageSize;
        public Integer page;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "markerDetails")
    @CrossOrigin
    @RequestMapping(value = {"/{database:.+}" + URL_BASE_PREFIX + "/" + URL_MARKERS_SEARCH}, method = {RequestMethod.POST}, produces = "application/json")
    public Map<String, Object> markers(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestBody MarkerSearchRequest requestBody) throws ObjectNotFoundException, Exception {
        return markers(request, response, database, requestBody.markerDbIds, requestBody.name, requestBody.matchMethod, requestBody.include, requestBody.type, requestBody.pageSize, requestBody.page);
    }

    /**
     * Cleanup old export data.
     *
     * @param request the request
     * @throws Exception
     */
    private void cleanupOldExportData(HttpServletRequest request) throws Exception {
        if (request.getSession() == null) {
            throw new Exception("Invalid request object");
        }

        long nowMillis = new Date().getTime();
        File filterOutputLocation = new File(servletContext.getRealPath(File.separator + TMP_OUTPUT_FOLDER));
        if (filterOutputLocation.exists() && filterOutputLocation.isDirectory()) {
            for (File f : filterOutputLocation.listFiles()) {
                if (!f.isDirectory() && nowMillis - f.lastModified() > EXPORT_FILE_EXPIRATION_DELAY_MILLIS) {
                    if (!f.delete()) {
                        LOG.warn("Unable to delete " + f.getPath());
                    } else {
                        LOG.info("BrAPI export file was deleted: " + f.getPath());
                    }
                }
            }
        }
    }

    static public class AlleleMatrixRequest {

        public Collection<String> markerprofileDbId;
        public List<Object> markerDbId;
        public String unknownString;
        public String sepUnphased;
        public String sepPhased;
        public Boolean expandHomozygotes;
        public String format;
        public Integer pageSize;
        public Integer page;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "alleleMatrix")
    @CrossOrigin
    @RequestMapping(method = RequestMethod.POST, value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_ALLELE_MATRIX, consumes = "application/json", produces = "application/json")
    public Map<String, Object> alleleMatrix(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestBody AlleleMatrixRequest requestBody) throws Exception {
        return alleleMatrix(request, response, database, requestBody.markerprofileDbId, requestBody.markerDbId, requestBody.unknownString, requestBody.sepUnphased, requestBody.sepPhased, requestBody.expandHomozygotes, requestBody.format, requestBody.pageSize, requestBody.page);
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "alleleMatrix")
    @CrossOrigin
    @RequestMapping(method = RequestMethod.GET, value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_ALLELE_MATRIX, produces = "application/json")
    public Map<String, Object> alleleMatrix(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestParam(name = "markerprofileDbId") Collection<String> markerprofileDbIDs, @RequestParam(name = "markerDbId", required = false) List<Object> markerDbIDs,
            @RequestParam(required = false) String unknownString, @RequestParam(required = false) String sepUnphased, @RequestParam(required = false) String sepPhased, @RequestParam(required = false) Boolean expandHomozygotes,
            @RequestParam(required = false) String format, @RequestParam(required = false) Integer pageSize, @RequestParam(required = false) Integer page) throws Exception {

        String token = tokenManager.readToken(request);
        if (!tokenManager.canUserReadDB(token, database)) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return null;
        }

        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }

//    	LOG.debug("alleleMatrix called");
        long before = System.currentTimeMillis();

        ArrayList<ArrayList<String>> data = new ArrayList<>();
        Map<String, Object> resultObject;

        TreeSet<String> sortedMarkerprofileDbIDs = new TreeSet<String>(new AlphaNumericComparator<String>());
        sortedMarkerprofileDbIDs.addAll(markerprofileDbIDs);
        Collection<GenotypingSample> samples = mongoTemplate.find(new Query(Criteria.where("_id").in(sortedMarkerprofileDbIDs.stream().map(id -> Integer.parseInt(id)).collect(Collectors.toList()))), GenotypingSample.class);

        List<Object> wantedMarkerIDs;
        if (markerDbIDs != null) {
            wantedMarkerIDs = markerDbIDs;
        } else {
            wantedMarkerIDs = new ArrayList<Object>();
            Document projectObject = new Document("_id", 1);
            MongoCursor<Document> markerCursor = mongoTemplate.getCollection(mongoTemplate.getCollectionName(VariantData.class)).find(new Document()).projection(projectObject).iterator();
            while (markerCursor.hasNext()) {
                wantedMarkerIDs.add(markerCursor.next().get("_id"));
            }
        }

        String unknownGtCode = unknownString == null ? "-" : unknownString;
        String unPhasedSeparator = sepUnphased == null ? "/" : sepUnphased;
        String phasedSeparator = sepPhased == null ? "|" : URLDecoder.decode(sepPhased, "UTF-8");

        if ("text/tsv".equalsIgnoreCase(format) || "tsv".equalsIgnoreCase(format)) {
            resultObject = getStandardResponse(0, 0, 0, 0, true);
            Status status = new Status();
            String extractId = System.currentTimeMillis() + Helper.convertToMD5(database + "__" + token);
            status.setCode("asynchid");
            status.setMessage(extractId);
            Metadata metadata = (Metadata) resultObject.get("metadata");
            metadata.setStatus(Arrays.asList(status));

            final List<Object> finalMarkerList = wantedMarkerIDs;
            new Thread() {
                public void run() {
                    ProgressIndicator progress = new ProgressIndicator(extractId, new String[]{"Generating export file"});
                    ProgressIndicator.registerProgressIndicator(progress);

                    int nChunkIndex = 0, nChunkSize = IExportHandler.computeQueryChunkSize(mongoTemplate, finalMarkerList.size());

                    FileWriter fw = null;
                    try {
                        String relativeOutputFolder = File.separator + TMP_OUTPUT_FOLDER + File.separator;
                        File outputLocation = new File(servletContext.getRealPath(relativeOutputFolder));
                        if (!outputLocation.exists() && !outputLocation.mkdirs()) {
                            throw new Exception("Unable to create folder: " + outputLocation);
                        }

                        fw = new FileWriter(new File(outputLocation.getAbsolutePath() + File.separator + extractId + ".tsv"));
                        fw.write("markerprofileDbIds\t" + StringUtils.join(sortedMarkerprofileDbIDs, "\t"));

                        HashMap<GenotypingSample, String> previousPhasingIds = new HashMap<>();
                        while (nChunkIndex * nChunkSize < finalMarkerList.size()) {
                            progress.setCurrentStepProgress((nChunkIndex * nChunkSize) * 100 / finalMarkerList.size());

                            List<Object> markerSubList = finalMarkerList.subList(nChunkIndex * nChunkSize, Math.min(finalMarkerList.size(), ++nChunkIndex * nChunkSize));
                            LinkedHashMap<VariantData, Collection<VariantRunData>> variantsAndRuns = MgdbDao.getSampleGenotypes(mongoTemplate, samples, markerSubList, true, null/*new Sort(Sort.Direction.DESC, "_id")*/);
                            VariantData[] variants = variantsAndRuns.keySet().toArray(new VariantData[variantsAndRuns.size()]);
                            for (int i = 0; i < variantsAndRuns.size(); i++) {
                                Collection<VariantRunData> runs = variantsAndRuns.get(variants[i]);
                                if (runs != null) {
                                    for (VariantRunData run : runs) {
                                        VariantRunDataId variantRunDataId = run.getId();
                                        fw.write(IExportHandler.LINE_SEPARATOR + variantRunDataId.getVariantId());
                                        for (GenotypingSample sample : samples) {
                                            SampleGenotype sampleGenotype = run.getSampleGenotypes().get(sample.getId());
                                            if (sampleGenotype == null) {
                                                fw.write("\t");
                                                continue;	// no data in this run + marker for that sample
                                            }

                                            String currentPhId = (String) sampleGenotype.getAdditionalInfo().get(VariantData.GT_FIELD_PHASED_ID);
                                            boolean fPhased = currentPhId != null && currentPhId.equals(previousPhasingIds.get(sample));
                                            previousPhasingIds.put(sample, currentPhId == null ? variantRunDataId.getVariantId() : currentPhId);

                                            String gtCode = sampleGenotype.getCode();
                                            if (gtCode.length() == 0) {
                                                fw.write("\t" + unknownGtCode);
                                            } else {
                                                List<String> alleles = variants[i].getAllelesFromGenotypeCode(gtCode);
                                                if (!Boolean.TRUE.equals(expandHomozygotes) && new HashSet<String>(alleles).size() == 1) {
                                                    fw.write("\t" + alleles.get(0));
                                                } else {
                                                    fw.write("\t" + StringUtils.join(alleles, fPhased ? phasedSeparator : unPhasedSeparator));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        progress.setCurrentStepProgress(100);
                        progress.markAsComplete();
                        LOG.debug("alleleMatrix took " + (System.currentTimeMillis() - before) / 1000d + "s");
                    } catch (Exception e) {
                        progress.setError("Error writing alleleMatrix to tsv file: " + e.getMessage());
                        LOG.error("Error writing alleleMatrix to tsv file", e);
                    } finally {
                        if (fw != null)
							try {
                            fw.close();
                        } catch (IOException ignored) {
                        }
                    }
                }
            }.start();

            cleanupOldExportData(request);
        } else {
            final int MAX_SUPPORTED_MATRIX_SIZE = 30000;

            if (pageSize == null || pageSize > MAX_SUPPORTED_MATRIX_SIZE) {
                pageSize = MAX_SUPPORTED_MATRIX_SIZE;
            }
            if (page == null) {
                page = 0;
            }

            int numberOfMarkersToReturn = (int) Math.ceil(pageSize / markerprofileDbIDs.size());
            int totalMarkerCount = (int) (markerDbIDs != null ? markerDbIDs.size() : Helper.estimDocCount(mongoTemplate, VariantData.class));

            wantedMarkerIDs = wantedMarkerIDs.subList(page * numberOfMarkersToReturn, Math.min(wantedMarkerIDs.size(), (page + 1) * numberOfMarkersToReturn));

            LinkedHashMap<VariantData, Collection<VariantRunData>> variantsAndRuns = MgdbDao.getSampleGenotypes(mongoTemplate, samples, wantedMarkerIDs, true, null/*new Sort(Sort.Direction.DESC, "_id")*/);	// query mongo db for matching genotypes
            VariantData[] variants = variantsAndRuns.keySet().toArray(new VariantData[variantsAndRuns.size()]);
            for (int i = 0; i < variantsAndRuns.size(); i++) {
                Collection<VariantRunData> runs = variantsAndRuns.get(variants[i]);
                if (runs != null) {
                    for (VariantRunData run : runs) {
                        for (Integer sampleId : run.getSampleGenotypes().keySet()) {
                            SampleGenotype sampleGenotype = run.getSampleGenotypes().get(sampleId);
                            String gtCode = sampleGenotype.getCode();	// we don't support exporting phasing information because of complexity due to pagination 
                            ArrayList<String> gtList = new ArrayList<String>();
                            gtList.add(variants[i].getId().toString());
                            gtList.add("" + sampleId);
                            if (gtCode.length() == 0) {
                                gtList.add(unknownGtCode);
                            } else {
                                List<String> alleles = variants[i].getAllelesFromGenotypeCode(gtCode);
                                if (!Boolean.TRUE.equals(expandHomozygotes) && new HashSet<String>(alleles).size() == 1) {
                                    gtList.add(alleles.get(0));
                                } else {
                                    gtList.add(StringUtils.join(alleles, unPhasedSeparator));
                                }
                            }
                            data.add(gtList);
                        }
                    }
                }
            }
            resultObject = getStandardResponse(variants.length * markerprofileDbIDs.size(), page, totalMarkerCount * markerprofileDbIDs.size(), numberOfMarkersToReturn * markerprofileDbIDs.size(), true);
            LOG.debug("alleleMatrix took " + (System.currentTimeMillis() - before) / 1000d + "s");
        }

        ((Map<String, Object>) resultObject.get("result")).put("data", data);
        return resultObject;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "alleleMatrixExportStatus")
    @CrossOrigin
    @RequestMapping(value = "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_ALLELE_MATRIX_STATUS + "/{extractID}", method = RequestMethod.GET, produces = "application/json")
    public Map<String, Object> alleleMatrixExportStatus(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @PathVariable String extractID) throws Exception {
        String token = tokenManager.readToken(request);
        if (!tokenManager.canUserReadDB(token, database) || !extractID.endsWith(Helper.convertToMD5(database + "__" + token))) {
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return null;
        }

        ProgressIndicator progress = ProgressIndicator.get(extractID);
        if (progress == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }

        Map<String, Object> resultObject = getStandardResponse(0, (int) (progress.getCurrentStepProgress()), 0, 0, true);
        Metadata metadata = (Metadata) resultObject.get("metadata");
        Status status = new Status();
        status.setCode("asynchstatus");
        boolean fGotError = progress.getError() != null;
        status.setMessage(progress.isComplete() ? "FINISHED" : (fGotError ? "FAILED" : "INPROCESS"));
        if (fGotError) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
        }

        if (progress.isComplete()) {
            String sWebAppRoot = appConfig.get("enforcedWebapRootUrl");
            String fileUrl = (sWebAppRoot == null ? BackOfficeController.determinePublicHostName(request) + request.getContextPath() : sWebAppRoot) + "/" + TMP_OUTPUT_FOLDER + "/" + extractID + ".tsv";
            metadata.setDatafiles(Arrays.asList(fileUrl));
        }

        metadata.setStatus(Arrays.asList(status));
        return resultObject;
    }

    static public class CreateTokenRequestBody {
        public String username;
        public String password;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "createToken", notes = "Generates a token using passed credentials")
    @CrossOrigin
    @RequestMapping(value = {"/{database:.+}" + URL_BASE_PREFIX + "/" + URL_TOKEN, "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_LOGIN_V1_3}, method = RequestMethod.POST, produces = "application/json"/*, consumes = "application/json"*/)
    public Map<String, Object> createToken(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestBody CreateTokenRequestBody userCredentials) throws IllegalArgumentException, IOException {
        /* FIXME: don't allow login if not in https? */
        MongoTemplate mongoTemplate = MongoTemplateManager.get(database);
        if (mongoTemplate == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return null;
        }
        
        if (userCredentials.username == null || userCredentials.username.isEmpty() ||  userCredentials.password == null || userCredentials.password.isEmpty()) {
            response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return null;
        }

        Map<String, Object> resultObject = getStandardResponse(0, 0, 0, 0, false);

        int maxInactiveIntervalInSeconds = request.getSession().getMaxInactiveInterval();
        if (maxInactiveIntervalInSeconds > 0)
            tokenManager.setSessionTimeoutInSeconds(maxInactiveIntervalInSeconds);

        try
        {
            Authentication authentication = authenticationManager.authenticate((new UsernamePasswordAuthenticationToken(userCredentials.username, userCredentials.password)));
            response.setStatus(HttpServletResponse.SC_CREATED);
            LOG.info("Successful authentication for user " + userCredentials.username);
            resultObject.put("access_token", tokenManager.generateToken(authentication));
            resultObject.put("expires_in", tokenManager.getSessionTimeoutInSeconds());
            resultObject.put("userDisplayName", userCredentials.username);
            return resultObject;
        
        }
        catch (BadCredentialsException ignored)
        {
            LOG.info("Authentication failed for user " + userCredentials.username);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return null;
        }
        finally {
            try {
                tokenManager.cleanupTokenMap();
            } catch (ParseException e) {
                LOG.warn("Error executing cleanupTokenMap", e);
            }

        }
    }

    static public class ClearTokenRequestBody {

        public String access_token;
    }

    @ApiOperation(authorizations = { @Authorization(value = "AuthorizationToken") }, value = "clearToken")
    @CrossOrigin
    @RequestMapping(value = {"/{database:.+}" + URL_BASE_PREFIX + "/" + URL_TOKEN, "/{database:.+}" + URL_BASE_PREFIX + "/" + URL_LOGOUT_V1_3}, method = RequestMethod.DELETE, produces = "application/json")
    public Map<String, Object> clearToken(HttpServletRequest request, HttpServletResponse response, @PathVariable String database, @RequestBody(required = false) ClearTokenRequestBody bodyToken) {
        String token = tokenManager.readToken(request);
        String bodyTokenValue = bodyToken != null ? bodyToken.access_token : null;
        int responseStatus;
        if (token == null || (bodyTokenValue != null && !token.equals(bodyTokenValue))) {
            responseStatus = HttpServletResponse.SC_BAD_REQUEST;
        } else if (!tokenManager.removeToken(token)) {
            responseStatus = HttpServletResponse.SC_GONE;
        } else {
            responseStatus = HttpServletResponse.SC_CREATED;
        }
        response.setStatus(responseStatus);
        if (responseStatus != HttpServletResponse.SC_CREATED) {
            LOG.debug("Token was not deleted: " + token);
            return null;
        }

//        tokenManager.removeToken(token);
//        SecurityContextHolder.clearContext();	// make it unretrievable
        Map<String, Object> resultObject = getStandardResponse(0, 0, 0, 0, false);
        Status status = new Status();
        status.setMessage("User has been logged out successfully.");
        Metadata metadata = (Metadata) resultObject.get("metadata");
        metadata.setStatus(Arrays.asList(status));
        LOG.debug("Deleted token: " + token);

        return resultObject;
    }

    public void build401Response(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        resp.getWriter().write("You are not allowed to access this resource");
    }

    public void build404Response(HttpServletResponse resp) throws IOException {
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
        resp.getWriter().write("This resource does not exist");
    }

    @Override
    public void setServletContext(ServletContext servletContext) {
        this.servletContext = servletContext;
    }
}
