package com.kineticdata.bridgehub.adapter.amazonecs;

import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.BridgeUtils;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.bridgehub.adapter.amazonec2.v2.AmazonEC2Adapter;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.LoggerFactory;


public class AmazonEcsAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/

    /** Defines the adapter display name */
    public static final String NAME = "Amazon ECS Bridge";

    /** Defines the logger */
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(AmazonEcsAdapter.class);

    /** Adapter version constant. */
    public static String VERSION;
    /** Load the properties version from the version.properties file. */
    static {
        try {
            java.util.Properties properties = new java.util.Properties();
            properties.load(AmazonEcsAdapter.class.getResourceAsStream("/"+AmazonEcsAdapter.class.getName()+".version"));
            VERSION = properties.getProperty("version");
        } catch (IOException e) {
            logger.warn("Unable to load "+AmazonEcsAdapter.class.getName()+" version properties.", e);
            VERSION = "Unknown";
        }
    }

    /** Defines the collection of property names for the adapter */
    public static class Properties {
        public static final String ACCESS_KEY = "Access Key";
        public static final String SECRET_KEY = "Secret Key";
        public static final String REGION = "Region";
        public static final String API_VERSION = "API Version";
    }

    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
        new ConfigurableProperty(Properties.ACCESS_KEY).setIsRequired(true),
        new ConfigurableProperty(Properties.SECRET_KEY).setIsRequired(true).setIsSensitive(true),
        new ConfigurableProperty(Properties.REGION).setIsRequired(true)
    );

    private String accessKey;
    private String secretKey;
    private String region;
    private AmazonEC2Adapter ec2Adapter = null;

    /**
     * Structures that are valid to use in the bridge
     */
    public static final List<String> VALID_STRUCTURES = Arrays.asList(new String[] {
        "Clusters","ContainerInstances","Tasks","TaskDefinitions"
    });

    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public void initialize() throws BridgeError {
        this.accessKey = properties.getValue(Properties.ACCESS_KEY);
        this.secretKey = properties.getValue(Properties.SECRET_KEY);
        this.region = properties.getValue(Properties.REGION);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getVersion() {
        return VERSION;
    }

    @Override
    public void setProperties(Map<String,String> parameters) {
        properties.setValues(parameters);
    }

    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }

    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public Count count(BridgeRequest request) throws BridgeError {
        String structure = request.getStructure();

        if (!VALID_STRUCTURES.contains(request.getStructure())) {
            throw new BridgeError("Invalid Structure: '" + request.getStructure() + "' is not a valid structure");
        }

        AmazonEcsQualificationParser parser = new AmazonEcsQualificationParser();
        String query = parser.parse(request.getQuery(),request.getParameters());

        // Build the response structure key identifier by lowercase the first letter of the structure
        String structureKeyIdentifier = structure.substring(0, 1).toLowerCase().concat(structure.substring(1,structure.length()-1));

        // Make the call to ECS to retrieve the Arns matching the query
        JSONObject arnsJson = ecsRequest("List"+structure,query);
        JSONArray structureArns = (JSONArray)arnsJson.get(structureKeyIdentifier.concat("Arns"));

        return new Count(structureArns.size());
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        String structure = request.getStructure();

        if (!VALID_STRUCTURES.contains(request.getStructure())) {
            throw new BridgeError("Invalid Structure: '" + request.getStructure() + "' is not a valid structure");
        }

        AmazonEcsQualificationParser parser = new AmazonEcsQualificationParser();
        String query = parser.parse(request.getQuery(),request.getParameters());

        // Build the response structure key identifier by lowercase the first letter of the structure
        String structureKeyIdentifier = structure.substring(0, 1).toLowerCase().concat(structure.substring(1,structure.length()-1));

        Matcher arnMatch = Pattern.compile("((?:"+structureKeyIdentifier+")?[Aa]rn=(.*?))(?:&|\\z)").matcher(query);
        if (arnMatch.find()) {
            String arn = arnMatch.group(2) == null ? "" : arnMatch.group(2);
            query = query.replace(arnMatch.group(1),structureKeyIdentifier.concat("Arns=["+arn+"]"));
        }

        request.setQuery(query);
        List<Record> records = search(request).getRecords();

        Record record;
        if (records.size() > 1) {
            throw new BridgeError("Multiple results matched an expected single match query");
        } else if (records.isEmpty()) {
            record = new Record(null);
        } else {
            record = records.get(0);
        }

        return record;
    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        String structure = request.getStructure();

        if (!VALID_STRUCTURES.contains(request.getStructure())) {
            throw new BridgeError("Invalid Structure: '" + request.getStructure() + "' is not a valid structure");
        }

        AmazonEcsQualificationParser parser = new AmazonEcsQualificationParser();
        String query = parser.parse(request.getQuery(),request.getParameters());

        // Initialize the nextPageToken and pageSize variables
        String nextPageToken = null;
        String pageSize = request.getMetadata("pageSize") == null ? "0" : request.getMetadata("pageSize");

        // Build the response structure key identifier by lowercase the first letter of the structure
        String structureKeyIdentifier = structure.substring(0, 1).toLowerCase().concat(structure.substring(1,structure.length()-1));

        Matcher arnsMatcher =Pattern.compile(structureKeyIdentifier+"Arns=\\[(.*?)\\]").matcher(query);
        // Keep the list of each chunk of structureArns that were retrieved (with a max of 100)
        // so that when the Describe call is done we don't try to describe more Arns that ECS
        // allows in one call
        List<List<String>> structureArnChunks = new ArrayList<List<String>>();
        if (arnsMatcher.find()) {
            String arns = arnsMatcher.group(1);
            structureArnChunks.add(Arrays.asList(arns.split(",")));
        } else {
            // Make the call to ECS to retrieve the Arns matching the query
            nextPageToken = request.getMetadata("pageToken");
            do {
                if (nextPageToken != null) {
                    if (!query.isEmpty()) query = query.concat("&");
                    query = query.concat("nextToken=").concat(nextPageToken);
                }
                if (!pageSize.equals("0")) {
                    if (!query.isEmpty()) query = query.concat("&");
                    query = query.concat("maxResults=").concat(pageSize);
                }

                JSONObject arnsJson = ecsRequest("List"+structure,query);
                nextPageToken = (String)arnsJson.get("nextToken");

                // Parse through and retrieve the structure Arns that match the query
                List<String> structureArns = new ArrayList<String>();
                JSONArray structureArnsJson = (JSONArray)arnsJson.get(structureKeyIdentifier.concat("Arns"));
                for (Object o : structureArnsJson) {
                    structureArns.add(o.toString());
                }
                structureArnChunks.add(structureArns);
            } while (nextPageToken != null && pageSize.equals("0"));
        }

        // Retrieve the cluster from the original query to append to the describe query (if it was
        // originally included)
        String cluster = null;
        if (!structure.equals("Clusters") && !structure.equals("TaskDefinitions")) {
            Matcher m = Pattern.compile("cluster=(.*?)(?:&|\\z)").matcher(query);
            if (m.find()) cluster = m.group(1);
        }

        List<Record> records = new ArrayList<Record>();
        for (List<String> structureArns : structureArnChunks) {
            // Make the call to ECS retrieve the record objects for the returned Arns
            JSONArray structureObjs;
            if (structure.equals("TaskDefinitions")) {
                // Make a different call for TaskDefinitions because it's List and Describe calls use
                // different singular/plural naming defintions unlike the other structures
                structureObjs = new JSONArray();
                for (String taskDefinitionArn : structureArns) {
                    JSONObject describeJson = ecsRequest("Describe"+structure.substring(0,structure.length()-1),"taskDefinition="+taskDefinitionArn);
                    JSONObject taskDefinition = (JSONObject)describeJson.get("taskDefinition");
                    structureObjs.add(taskDefinition);
                }
            } else {
                StringBuilder describeQuery = new StringBuilder();
                describeQuery.append(structureKeyIdentifier).append("s=[").append(StringUtils.join(structureArns,",")).append("]");
                if (cluster != null) describeQuery.append("&cluster=").append(cluster);
                JSONObject describeJson = ecsRequest("Describe"+structure,describeQuery.toString());
                structureObjs = (JSONArray)describeJson.get(structureKeyIdentifier.concat("s"));
            }
            // Parse through the response JSON to build record objects
            for (Object o : structureObjs) {
                records.add(new Record((Map)o));
            }
        }

        // Get other structure fields add to the record objects if they were included in the
        // fields list or were included as a field to query by
        Matcher matchStructureFields = Pattern.compile("(?:\\A|&)([^&]*?\\..*?)=(?:.*?)(?:\\z|&)").matcher(query);
        List<String> retrievalFields = new ArrayList<String>();
        while (matchStructureFields.find()) {
            retrievalFields.add(matchStructureFields.group(1));
        }

        if ((request.getFields() != null && !request.getFields().isEmpty()) || !retrievalFields.isEmpty()) {
            if (request.getFields() != null) retrievalFields.addAll(request.getFields());
            records = addOtherStructureFields(retrievalFields,records,cluster);
        }

        // Define the fields - if not fields were passed, set they keySet of the a returned objects as
        // the field set
        List<String> fields = request.getFields();
        Map<String,String> aliasedFields = new HashMap<String,String>();
        if ((fields == null || fields.isEmpty()) && !records.isEmpty()) {
            fields = new ArrayList<String>(records.get(0).getRecord().keySet());
        } else {
            for (String field : fields) {
                String aliasedField = aliasedField(field);
                if (!field.equals(aliasedField)) aliasedFields.put(field,aliasedField);
            }
        }

        for (String field : fields) {
            String aliasedField = aliasedFields.containsKey(field) ? aliasedFields.get(field) : field;
            if (field.matches(NESTED_PATTERN.pattern())) {
                // Parse the base field and subfields from the field string
                String base = aliasedField.substring(0,aliasedField.indexOf("["));
                Matcher matcher = NESTED_PATTERN.matcher(aliasedField);
                List<String> subfields = new ArrayList<String>();
                while (matcher.find()) {
                    subfields.add(matcher.group(1));
                }

                // Make a copy of the record to step through and put the value of the base field
                // in the "valuesToCheck" list
                for (Record record : records) {
                    List valuesToCheck = new ArrayList();
                    valuesToCheck.add(record.getValue(base));
                    for (String subfield : subfields) {
                        valuesToCheck = getSubfieldValues(subfield, valuesToCheck);
                    }

                    // Remove any null values are are currently in valuesToCheck
                    valuesToCheck.removeAll(Collections.singleton(null));
                    if (valuesToCheck.size() > 1) {
                        record.getRecord().put(field,valuesToCheck);
                    } else if (valuesToCheck.size() == 1) {
                        record.getRecord().put(field,valuesToCheck.get(0));
                    } else {
                        record.getRecord().put(field,null);
                    }
                }
            }
        }

        // Filter and sort the records
        records = filterRecords(records,query);
        if (request.getMetadata("order") == null) {
            // name,type,desc assumes name ASC,type ASC,desc ASC
            Map<String,String> defaultOrder = new LinkedHashMap<String,String>();
            for (String field : fields) {
                defaultOrder.put(field, "ASC");
            }
            records = BridgeUtils.sortRecords(defaultOrder, records);
        } else {
            // Creates a map out of order metadata
            Map<String,String> orderParse = BridgeUtils.parseOrder(request.getMetadata("order"));
            records = BridgeUtils.sortRecords(orderParse, records);
        }

        // Define the metadata
        Map<String,String> metadata = new LinkedHashMap<String,String>();
        metadata.put("size",String.valueOf(records.size()));
        metadata.put("pageSize",pageSize);
        metadata.put("nextPageToken",nextPageToken);

        // Returning the response
        return new RecordList(fields, records, metadata);
    }

    /*----------------------------------------------------------------------------------------------
     * HELPER METHODS
     *--------------------------------------------------------------------------------------------*/

    private JSONObject ecsRequest(String action, String query) throws BridgeError {
        // Build up the request query into a JSON object
        Map<String,Object> jsonQuery = new HashMap<String,Object>();
        if (query != null && !query.isEmpty()) {
            for (String part : query.split("&")) {
                String[] keyValue = part.split("=");
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();
                // If the value is surrounded by [ ] it should be turned into a string list
                if (value.startsWith("[") && value.endsWith("]")) {
                    jsonQuery.put(key,Arrays.asList(value.substring(1,value.length()-1).split(",")));
                } else if (key.equals("maxResults")) {
                    jsonQuery.put(key,Integer.valueOf(value));
                } else {
                    jsonQuery.put(key,value);
                }
            }
        }

        // The headers that we want to add to the request
        List<String> headers = new ArrayList<String>();
        headers.add("Content-Type: application/x-amz-json-1.1");
        headers.add("x-amz-target: AmazonEC2ContainerServiceV20141113."+action);

        // Make the request using the built up url/headers and bridge properties
        HttpResponse response = request("POST","https://ecs."+this.region+".amazonaws.com",headers,this.region,"ecs",JSONValue.toJSONString(jsonQuery),this.accessKey,this.secretKey);
        String output;
        try {
            output = EntityUtils.toString(response.getEntity());
        } catch (IOException e) { throw new BridgeError(e); }

        JSONObject json = (JSONObject)JSONValue.parse(output);
        if (json.containsKey("__type")) {
            logger.error(output);
            StringBuilder errorMessage = new StringBuilder("Error retrieving ECS records (See logs for more details)");
            errorMessage.append(" -- Type: ").append(json.get("__type").toString());
            if (json.containsKey("Message")) errorMessage.append(" -- Message: ").append(json.get("Message").toString());
            throw new BridgeError(errorMessage.toString());
        }

        return json;
    }

    private List<Record> addOtherStructureFields(List<String> fields, List<Record> records, String cluster) throws BridgeError {
        // Build hash of arns that should be returned from other structures
        Map<String,Map<String,Object>> complexObjects = new HashMap<String,Map<String,Object>>();

        // Find any complex fields
        Map<String,ArrayList<String>> complexFields = new HashMap<String,ArrayList<String>>();
        Pattern pattern = Pattern.compile("(.*?)\\.(.*?)\\z");
        for (String field : fields) {
            Matcher m = pattern.matcher(field);
            if (m.find()) {
                if (complexFields.containsKey(m.group(1))) {
                    complexFields.get(m.group(1)).add(m.group(2));
                } else {
                    complexFields.put(m.group(1), new ArrayList(Arrays.asList(m.group(2))));
                    complexObjects.put(m.group(1),new HashMap<String,Object>());
                }
            }
        }

        if (!complexFields.isEmpty()) {
            // Retrieve the arns for the complex fields that need to be called
            Set<String> complexKeys = complexFields.keySet();
            for (Record record :records) {
                for (String key : complexKeys) {
                    String arn;
                    if (key.equals("instance")) {
                        arn = (String)record.getValue("ec2InstanceId");
                    } else {
                        arn = (String)record.getValue(key.concat("Arn"));
                    }
                    if (!complexObjects.get(key).keySet().contains(arn)) complexObjects.get(key).put(arn,null);
                }
            }

            // Make the calls to the different structures and populate the complexObject field
            // with the object corresponding to each arn
            for (Map.Entry<String,ArrayList<String>> entry : complexFields.entrySet()) {
                // Add the structureId to the list of fields to return
                String structureId = entry.getKey().equals("instance") ? "instanceId" : entry.getKey().concat("Arn");
                entry.getValue().add(structureId);

                RecordList recordList;
                if (entry.getKey().equals("instance")) {
                    if (ec2Adapter == null) {
                        ec2Adapter = new AmazonEC2Adapter();
                        ec2Adapter.setProperties(getProperties().getValues());
                        ec2Adapter.initialize();
                    }
                    BridgeRequest request = new BridgeRequest();
                    request.setStructure("Instances");
                    request.setFields(entry.getValue());
                    int instanceIdCount = 1;
                    Set<String> instanceIds = complexObjects.get(entry.getKey()).keySet();
                    StringBuilder query = new StringBuilder();
                    for (String instanceId : instanceIds) {
                        query.append("InstanceId.").append(instanceIdCount).append("=");
                        query.append(instanceId);
                        instanceIdCount++;
                        if (instanceIdCount != instanceIds.size()+1) query.append("&");
                    }
                    request.setQuery(query.toString());
                    recordList = ec2Adapter.search(request);
                } else {
                    // Build structure
                    StringBuilder structure = new StringBuilder();
                    structure.append(entry.getKey().substring(0,1).toUpperCase());
                    structure.append(entry.getKey().substring(1)).append("s");
                    // Build query
                    StringBuilder complexQuery = new StringBuilder();
                    complexQuery.append(entry.getKey()).append("s=[");
                    complexQuery.append(StringUtils.join(complexObjects.get(entry.getKey()).keySet(),","));
                    complexQuery.append("]");
                    if (cluster != null) complexQuery.append("&cluster=").append(cluster);
                    // Build the BridgeRequest
                    BridgeRequest request = new BridgeRequest();
                    request.setStructure(structure.toString());
                    request.setQuery(complexQuery.toString());
                    request.setFields(entry.getValue());
                    // Make the request
                    recordList = search(request);
                }

                for (Record record : recordList.getRecords()) {
                    String arn = record.getValue(structureId).toString();
                    complexObjects.get(entry.getKey()).put(arn,record.getRecord());
                }
            }

            // Retrieve the objects related to the complex fields based on the corresponding Arn
            for (Record record : records) {
                for (Map.Entry<String,ArrayList<String>> entry : complexFields.entrySet()) {
                    for (String complexField : entry.getValue()) {
                        String arn = entry.getKey().equals("instance") ? record.getValue("ec2InstanceId").toString() : record.getValue(entry.getKey().concat("Arn")).toString();
                        Map json = (Map)complexObjects.get(entry.getKey()).get(arn);
                        record.getRecord().put(entry.getKey()+"."+complexField,json.get(complexField));
                    }
                }
            }
        }

        return records;
    }

    private static final Pattern NESTED_PATTERN = Pattern.compile(".*?\\[(.*?)\\]");
    private List getSubfieldValues(String subfield, List valuesToCheck) {
        List subfieldValues = new ArrayList(valuesToCheck);
        for (Object value : new ArrayList(subfieldValues)) {
            if (value instanceof Map<?,?>) {
                subfieldValues.remove(value);
                subfieldValues.add(((Map)value).get(subfield));
            } else if (value instanceof List<?>) {
                subfieldValues.remove(value);
                // Check if it is a list of name/value hashes
                List nonNameValuePairObjects = new ArrayList();
                for (Object o : (List)value) {
                    if (o instanceof Map) {
                        if (((Map) o).containsKey("name") && ((Map) o).containsKey("value")) {
                            if (((Map) o).get("name").toString().equals(subfield)) {
                                subfieldValues.add(((Map) o).get("value"));
                            }
                        } else {
                            nonNameValuePairObjects.add(o);
                            break;
                        }
                    }
                }
                // If it is not a list of name/value hashes
                if (!nonNameValuePairObjects.isEmpty()) {
                    subfieldValues.addAll(getSubfieldValues(subfield,nonNameValuePairObjects));
                }
            } else {
                subfieldValues.remove(value);
                break;
            }
        }
        return subfieldValues;
    }

    private List<Record> filterRecords(List<Record> records, String query) {
        String[] queryParts = query.split("&");

        Set<String> fields = null;
        if (!records.isEmpty()) fields = records.get(0).getRecord().keySet();
        List<Record> filteredRecords = new ArrayList<Record>(records);
        for (String part : queryParts) {
            String[] parts = part.split("=");
            String key = aliasedField(parts[0].trim());
            String value = parts.length == 1 ? "" : parts[1].trim();

            for (Record record : new ArrayList<Record>(filteredRecords)) {
                if (fields.contains(key) && !value.equals(record.getValue(key))) {
                    // If doing client side filtering on a standard field and it doesn't match
                    filteredRecords.remove(record);
                } else if (!fields.contains(key) && key.matches(NESTED_PATTERN.pattern())) {
                    // Parse the base field and subfields from the field string
                    String base = key.substring(0,key.indexOf("["));
                    Matcher matcher = NESTED_PATTERN.matcher(key);
                    List<String> subfields = new ArrayList<String>();
                    while (matcher.find()) {
                        subfields.add(matcher.group(1));
                    }

                    // Make a copy of the record to step through and put the value of the base field
                    // in the "valuesToCheck" list
                    List valuesToCheck = Arrays.asList(record.getValue(base));
                    for (String subfield : subfields) {
                        valuesToCheck = getSubfieldValues(subfield, valuesToCheck);
                    }

                    // Iterate through the list of values, and if all of them fail to match remove the record
                    boolean valueMatch = false;
                    for (Object valueCheck : valuesToCheck) {
                        if (value.equals(valueCheck)) {
                            valueMatch = true;
                            break;
                        }
                    }
                    if (!valueMatch) filteredRecords.remove(record);
                }
            }
        }
        return filteredRecords;
    }

    // With the field pattern aliases, group the part of the query that you want to be aliases/replaced.
    // For example, environment[space_slug] => overrides[containerOverrides][environment][space_slug]
    // will be grouped on environment because that is what will be replaced by the real field.
    private static final Map<Pattern,String> FLD_PATTERN_ALIASES = new HashMap<Pattern,String>() {{
        put(Pattern.compile("(environment)\\[.*?\\]"),"overrides[containerOverrides][environment]");
    }};
    /**
     * Returns the full field that is being aliased (if there is one). Echos back the passed in field
     * if it is not representing an aliased value.
     * @param fieldName A field alias
     * @return The full field path
     */
    private String aliasedField(String fieldName) {
        String aliasedField = fieldName;
        // Check for complete field matches that are being aliased

        // Check for pattern matched aliases
        for (Map.Entry<Pattern,String> entry : FLD_PATTERN_ALIASES.entrySet()) {
            Matcher m = entry.getKey().matcher(fieldName);
            if (m.find()) aliasedField = aliasedField.replace(m.group(1), entry.getValue());
        }
        return aliasedField;
    }

    private List<String> aliasedFields(List<String> fieldNames) {
        List<String> aliasedFields = new ArrayList<String>();
        for (String fieldName : fieldNames) {
            aliasedFields.add(aliasedField(fieldName));
        }
        return aliasedFields;
    }

    /**
     * This method builds and sends a request to the Amazon EC2 REST API given the inputted
     * data and return a HttpResponse object after the call has returned. This method mainly helps with
     * creating a proper signature for the request (documentation on the Amazon REST API signing
     * process can be found here - http://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html),
     * but it also throws and logs an error if a 401 or 403 is retrieved on the attempted call.
     *
     * @param url
     * @param headers
     * @param region
     * @param accessKey
     * @param secretKey
     * @return
     * @throws BridgeError
     */
    private HttpResponse request(String method, String url, List<String> headers, String region, String service, String payload, String accessKey, String secretKey) throws BridgeError {
        // Build a datetime timestamp of the current time (in UTC). This will be sent as a header
        // to Amazon and the datetime stamp must be within 5 minutes of the time on the
        // recieving server or else the request will be rejected as a 403 Forbidden
        DateFormat df = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        String datetime = df.format(new Date());
        String date = datetime.split("T")[0];

        // Create a URI from the request URL so that we can pull the host/path/query from it
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new BridgeError("There was an error parsing the inputted url '"+url+"' into a java URI.",e);
        }

        /* BUILD CANONCIAL REQUEST (uri, query, headers, signed headers, hashed payload)*/

        // Canonical URI (the part of the URL between the host and the ?. If blank, the uri is just /)
        String canonicalUri = uri.getPath().isEmpty() ? "/" : uri.getPath();

        // Canonical Query (parameter names sorted by asc and param names and values escaped
        // and trimmed)
        String canonicalQuery;
        // Trim the param names and values and load the parameters into a map
        Map<String,String> queryMap = new HashMap<String,String>();
        if (uri.getQuery() != null) {
            for (String parameter : uri.getQuery().split("&")) {
                queryMap.put(parameter.split("=")[0].trim(), parameter.split("=")[1].trim());
            }
        }

        StringBuilder queryBuilder = new StringBuilder();
        for (String key : new TreeSet<String>(queryMap.keySet())) {
            if (!queryBuilder.toString().isEmpty()) queryBuilder.append("&");
            queryBuilder.append(URLEncoder.encode(key)).append("=").append(URLEncoder.encode(queryMap.get(key)));
        }
        canonicalQuery = queryBuilder.toString();

        // Canonical Headers (lowercase and sort headers, add host and date headers if they aren't
        // already included, then create a header string with trimmed name and values and a new line
        // character after each header - including the last one)
        String canonicalHeaders;
        // Lowercase/trim each header and header value and load into a map
        Map<String,String> headerMap = new HashMap<String,String>();
        for (String header : headers) {
            headerMap.put(header.split(":")[0].toLowerCase().trim(), header.split(":")[1].trim());
        }
        // If the date and host headers aren't already in the header map, add them
        if (!headerMap.keySet().contains("host")) headerMap.put("host",uri.getHost());
        if (!headerMap.keySet().contains("x-amz-date")) headerMap.put("x-amz-date",datetime);
        // Sort the headers and append a newline to the end of each of them
        StringBuilder headerBuilder = new StringBuilder();
        for (String key : new TreeSet<String>(headerMap.keySet())) {
            headerBuilder.append(key).append(":").append(headerMap.get(key)).append("\n");
        }
        canonicalHeaders = headerBuilder.toString();

        // Signed Headers (a semicolon separated list of heads that were signed in the previous step)
        String signedHeaders = StringUtils.join(new TreeSet<String>(headerMap.keySet()),";");

        // Hashed Payload (a SHA256 hexdigest with the request payload - because the bridge only
        // does GET requests the payload will always be an empty string)
        String hashedPayload = DigestUtils.sha256Hex(payload);

        // Canonical Request (built out of 6 parts - the request method and the previous 5 steps in order
        // - with a newline in between each step and then a SHA256 hexdigest run on the resulting string)
        StringBuilder requestBuilder = new StringBuilder();
        requestBuilder.append(method).append("\n");
        requestBuilder.append(canonicalUri).append("\n");
        requestBuilder.append(canonicalQuery).append("\n");
        requestBuilder.append(canonicalHeaders).append("\n");
        requestBuilder.append(signedHeaders).append("\n");
        requestBuilder.append(hashedPayload);

        logger.debug(requestBuilder.toString());
        // Run the resulting string through a SHA256 hexdigest
        String canonicalRequest = DigestUtils.sha256Hex(requestBuilder.toString());

        /* BUILD STRING TO SIGN (credential scope, string to sign) */

        // Credential Scope (date, region, service, and terminating string [which is always aws4_request)
        String credentialScope = String.format("%s/%s/%s/aws4_request",date,region,service);

        // String to Sign (encryption method, datetime, credential scope, and canonical request)
        StringBuilder stringToSignBuilder = new StringBuilder();
        stringToSignBuilder.append("AWS4-HMAC-SHA256").append("\n");
        stringToSignBuilder.append(datetime).append("\n");
        stringToSignBuilder.append(credentialScope).append("\n");
        stringToSignBuilder.append(canonicalRequest);
        logger.debug(stringToSignBuilder.toString());
        String stringToSign = stringToSignBuilder.toString();

        /* CREATE THE SIGNATURE (signing key, signature) */

        // Signing Key
        byte[] signingKey;
        try {
            signingKey = getSignatureKey(secretKey,date,region,service);
        } catch (Exception e) {
            throw new BridgeError("There was a problem creating the signing key",e);
        }

        // Signature
        String signature;
        try {
            signature = Hex.encodeHexString(HmacSHA256(signingKey,stringToSign));
        } catch (Exception e) {
            throw new BridgeError("There was a problem creating the signature",e);
        }

        // Authorization Header (encryption method, access key, credential scope, signed headers, signature))
        String authorization = String.format("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",accessKey,credentialScope,signedHeaders,signature);

        /* CREATE THE HTTP REQUEST */
        HttpClient client = HttpClients.createDefault();
        HttpRequestBase request;
        try {
            if (method.toLowerCase().equals("get")) {
                request = new HttpGet(url);
            } else if (method.toLowerCase().equals("post")) {
                request = new HttpPost(url);
                ((HttpPost)request).setEntity(new StringEntity(payload));
            } else {
                throw new BridgeError("Http Method '"+method+"' is not supported");
            }
        } catch (UnsupportedEncodingException e) {
            throw new BridgeError(e);
        }

        request.setHeader("Authorization",authorization);
        for (Map.Entry<String,String> header : headerMap.entrySet()) {

            request.setHeader(header.getKey(),header.getValue());
        }

        HttpResponse response;
        try {
            response = client.execute(request);

            if (response.getStatusLine().getStatusCode() == 401 || response.getStatusLine().getStatusCode() == 403) {
                logger.error(EntityUtils.toString(response.getEntity()));
                throw new BridgeError("User not authorized to access this resource. Check the logs for more details.");
            }
        } catch (IOException e) { throw new BridgeError(e); }

        return response;
    }

    static byte[] HmacSHA256(byte[] key, String data) throws Exception {
        String algorithm = "HmacSHA256";
        Mac mac = Mac.getInstance(algorithm);
        mac.init(new SecretKeySpec(key, algorithm));
        return mac.doFinal(data.getBytes("UTF8"));
    }

    static byte[] getSignatureKey(String secretKey, String date, String region, String service) throws Exception  {
         byte[] kSecret = ("AWS4" + secretKey).getBytes("UTF8");
         byte[] kDate    = HmacSHA256(kSecret, date);
         byte[] kRegion  = HmacSHA256(kDate, region);
         byte[] kService = HmacSHA256(kRegion, service);
         byte[] kSigning = HmacSHA256(kService, "aws4_request");
         return kSigning;
    }

}