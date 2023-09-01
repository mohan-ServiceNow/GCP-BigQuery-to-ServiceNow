

var raxDataJSON = new BigQueryConnectUtils().getData(); //Script include to connect GCP 
var raxDataParse = JSON.parse(raxDataJSON);

//return columns data in format of object for example : [{"name": "email","type": "STRING","mode": "NULLABLE"},{"name": "level00","type": "STRING","mode":"NULLABLE"}]
var columnsJSON = raxDataParse.schema.fields;

var columnsTitleValue = []; //store columns title name in array

for (var i = 0; i < columnsJSON.length; i++) {
    columnsTitleValue.push(columnsJSON[i].name);
}


var totalCount = raxDataParse.totalRows; //total count in BigQuery table
var rowsRaxData = raxDataParse.rows; //its return array

function getSysID(userName) {
    if (userName) {        
        var user = new GlideRecord('sys_user');
        user.addQuery('name', check_name);
        user.query();
        if (user.next()) {
            return user.getValue('sys_id');
        }
    }else{
		return '';
	}
}

//iterating through the Racker Roaster table in GCP BigQuery to insert or update data in the user org table
for (var x = 0; x < rowsRaxData.length; x++) {
	
	/* 
	
	The table below provides a guide for iterating the JSON data from BigQuery's roaster table.
	|=====================================|
	| Fields                        INDEX |
	|=====================================|
	| work_email                -->   0   |   
	| first_name                -->   1   |   
	| last_name                 -->   2   |   
	| preferred_name            -->   3   |   
	| workers_manager           -->   4   |   
	| sso                       -->   5   |   
	| job_profile               -->   6   |   
	| cch_l1                    -->   7   |   
	| cch_l2                    -->   8   |   
	| cch_l3                    -->   9   |   
	| cch_l4                    -->   10  |   
	| cch_l5                    -->   11  |   
	| cch_l6                    -->   12  |   
	| cch_l7                    -->   13  |   
	|=====================================|

*/
    var user = new GlideRecord('sys_user');
    user.addQuery('u_email_id', rowsRaxData[x].f[0].v);
    user.addQuery('u_first_name', rowsRaxData[x].f[1].v);
    user.addQuery('u_last_name', rowsRaxData[x].f[2].v);
    user.addQuery('u_name', rowsRaxData[x].f[3].v).addOrCondition('u_user_id', rowsRaxData[x].f[5].v);
    user.query();

    //  If duplicate records are discovered, then data in user org table will be updated. 
    if (user.next()) {

        user.u_manager = getSysID(rowsRaxData[x].f[4].v);
        user.u_job_title = rowsRaxData[x].f[6].v; //job_profile
        user.u_channel_1 = rowsRaxData[x].f[7].v; //cch_l1
        user.u_channel_2 = rowsRaxData[x].f[8].v; //cch_l2
        user.u_channel_3 = rowsRaxData[x].f[9].v; //cch_l3
        user.u_channel_4 = rowsRaxData[x].f[10].v; //cch_l4
        user.u_channel_5 = rowsRaxData[x].f[11].v; //cch_l5
        user.u_channel_6 = rowsRaxData[x].f[12].v; //cch_l6
        user.u_channel_7 = rowsRaxData[x].f[13].v; //cch_l7
        user.update();
    }

    // 	If duplicate records are not discovered, then data in user org table will be inserted. 
    else {
        var insertUser = new GlideRecord('sys_user');
        insertUser.initialize();
        insertUser.u_email_id = rowsRaxData[x].f[0].v; //worker_email 
        insertUser.u_first_name = rowsRaxData[x].f[1].v; //first_name 
        insertUser.u_last_name = rowsRaxData[x].f[2].v; //last_name 
        insertUser.u_name = rowsRaxData[x].f[3].v; //preferred_name 
        insertUser.u_racker_name = getSysID(rowsRaxData[x].f[3].v);
        insertUser.u_manager = getSysID(rowsRaxData[x].f[4].v); //work_manager
        insertUser.u_user_id = rowsRaxData[x].f[5].v; //sso
        insertUser.u_job_title = rowsRaxData[x].f[6].v; //job_profile
        insertUser.u_channel_1 = rowsRaxData[x].f[7].v; //cch_l1
        insertUser.u_channel_2 = rowsRaxData[x].f[8].v; //cch_l2
        insertUser.u_channel_3 = rowsRaxData[x].f[9].v; //cch_l3
        insertUser.u_channel_4 = rowsRaxData[x].f[10].v; //cch_l4
        insertUser.u_channel_5 = rowsRaxData[x].f[11].v; //cch_l5
        insertUser.u_channel_6 = rowsRaxData[x].f[12].v; //cch_l6
        insertUser.u_channel_7 = rowsRaxData[x].f[13].v; //cch_l7
        insertUser.insert();
    }
}
