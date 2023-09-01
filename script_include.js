var BigQueryConnectUtils = Class.create();
BigQueryConnectUtils.prototype = {
        initialize: function () { },

        getData: function () {

                var jwtAPI = new sn_auth.GlideJWTAPI();

                //         Header for JWT token 
                var headerJSON = {
                        typ: "JWT",
                        alg: "RSA256"
                };
                var header = JSON.stringify(headerJSON);

                //         claims for JWT where iss --> issuer (service account from GCP)

                // 		gdo-gcp-snow-svc@rax-landing.iam.gserviceaccount.com
                var payloadJSON = {
                        "iss": "******************Service Account***************************",
                        "sub": "******************Service Account***************************",
                        "scope": "https://www.googleapis.com/auth/bigquery",
                        "aud": "https://oauth2.googleapis.com/token",
                        "exp": 1328554385,
                        "iat": 1328550785
                };
               
                var payload = JSON.stringify(payloadJSON);

                //         sys_id of JWT provider
                var jwtProviderSysId = "c983d991db1be5103f64d157f49619d1";

                // 		Generating JWT token with header, claims and signature 
                var jwt = jwtAPI.generateJWT(jwtProviderSysId, header, payload);

                gs.info("JWT:" + jwt); //JWT token was created with encrpyted format containing header.claims.signature

                // 		 Genarating Access token with help of JWT token 
                var genToken = new sn_ws.RESTMessageV2();
                genToken.setHttpMethod('POST');
                genToken.setEndpoint('https://oauth2.googleapis.com/token');
                genToken.setQueryParameter('grant_type', 'urn:ietf:params:oauth:grant-type:jwt-bearer');
                genToken.setQueryParameter('assertion', jwt);
                genToken.setRequestHeader('content-type', 'application/x-www-form-urlencoded');
                var response = genToken.execute();

                var statusOfAccessToken = response.getStatusCode();
                var responseBody = response.getBody();
                var parseData = JSON.parse(responseBody);

                var accessToken = parseData.access_token;

                // The SQL commands in the following object will be run in the GCP BigQuery console by injecting them into the body.
                /* In query passing SQL Commands, only a few fields and their data from BigQuery GCP racker_roaster table will be returned. the fields are
                
                        
                */
                var body = {
                        "query": "select * from spotlight-analytics-388311.test.roster;",
                        "useLegacySql": false
                };

                // 		Accessing the endpoint with the access token created in JSON format in line 44, and then parsing the JSON saved access token in line 47.
                var raxRoasterTable = new sn_ws.RESTMessageV2();
                raxRoasterTable.setEndpoint('https://bigquery.googleapis.com/bigquery/v2/projects/{project_name}/queries');
                raxRoasterTable.setHttpMethod('POST');
                raxRoasterTable.setRequestHeader('authorization', 'Bearer ' + accessToken);
                raxRoasterTable.setRequestHeader("accept", "application/json");
                raxRoasterTable.setRequestHeader("content-type", "application/json");

                // 		Passing SQL Commands in body to run in GCP console
                raxRoasterTable.setRequestBody(JSON.stringify(body));

                var raxResponse = raxRoasterTable.execute();
                var statusOfRaxRoaster = raxResponse.getStatusCode();
                var raxData = raxResponse.getBody();

                return raxData;
        },

        type: 'BigQueryConnectUtils'
};
