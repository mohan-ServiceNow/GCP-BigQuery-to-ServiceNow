/*
The Purpose of this Scheduled job is inital load data in cto org table  
*/


var raxDataJSON = new BigQueryConnectUtils().getData(); //Script include to connect GCP 
var raxDataParse = JSON.parse(raxDataJSON);

//return columns data in format of object for example : [{"name": "email","type": "STRING","mode": "NULLABLE"},{"name": "level00","type": "STRING","mode":"NULLABLE"}]
var columnsJSON = raxDataParse.schema.fields;

var columnsTitleValue = []; //store columns title name in array

for (var i = 0; i < columnsJSON.length; i++) {
    columnsTitleValue.push(columnsJSON[i].name);
}
// for (var j = 0; j < columnsTitleValue.length; j++) {
//     gs.info(columnsJSON[j])
// }

// gs.info(columns_title.length);

var totalCount = raxDataParse.totalRows; //total count in BigQuery racker_roaster table
var rowsRaxData = raxDataParse.rows; //its return array

// This function returns the user's sys_id in order to populate the reference field, as well as deleting the (On Leave) string from the user name data in GCP.
function getSysID(userName) {
    if (userName) {
        var check_name = userName.includes('(On Leave)') ? userName.replaceAll('(On Leave)', '') : userName;
        check_name = check_name.includes('Contreras') ? check_name.replaceAll('Contreras', '') : check_name;
        
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

//iterating through the Racker Roaster table in GCP BigQuery to insert or update data in the CTO org table
for (var x = 0; x < rowsRaxData.length; x++) {
	
	/* 
	
	The table below provides a guide for iterating the JSON data from BigQuery's racker_roaster table.
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
    var cto = new GlideRecord('u_cto_org_data');
    //checking email from CTO org  table and BigQuery rax-landing.racker_roaster
    cto.addQuery('u_email_id', rowsRaxData[x].f[0].v);

    //checking first_name and last_name from CTO org table and BigQuery rax-landing.racker_roaster
    cto.addQuery('u_first_name', rowsRaxData[x].f[1].v);
    cto.addQuery('u_last_name', rowsRaxData[x].f[2].v);

    //checking preferred_name and sso from CTO org table and BigQuery rax-landing.racker_roaster
    cto.addQuery('u_name', rowsRaxData[x].f[3].v).addOrCondition('u_user_id', rowsRaxData[x].f[5].v);
    cto.query();

    //  If duplicate records are discovered, then data in CTO org table will be updated. 
    if (cto.next()) {

        cto.u_manager = getSysID(rowsRaxData[x].f[4].v);
        cto.u_job_title = rowsRaxData[x].f[6].v; //job_profile
        cto.u_channel_1 = rowsRaxData[x].f[7].v; //cch_l1
        cto.u_channel_2 = rowsRaxData[x].f[8].v; //cch_l2
        cto.u_channel_3 = rowsRaxData[x].f[9].v; //cch_l3
        cto.u_channel_4 = rowsRaxData[x].f[10].v; //cch_l4
        cto.u_channel_5 = rowsRaxData[x].f[11].v; //cch_l5
        cto.u_channel_6 = rowsRaxData[x].f[12].v; //cch_l6
        cto.u_channel_7 = rowsRaxData[x].f[13].v; //cch_l7
        cto.u_management_chain_level_00 = getSysID(rowsRaxData[x].f[14].v); //management_chain_level_00
        cto.u_management_chain_level_01 = getSysID(rowsRaxData[x].f[15].v); //management_chain_level_01
        cto.u_management_chain_level_02 = getSysID(rowsRaxData[x].f[16].v); //management_chain_level_02
        cto.u_management_chain_level_03 = getSysID(rowsRaxData[x].f[17].v); //management_chain_level_03
        cto.u_management_chain_level_04 = getSysID(rowsRaxData[x].f[18].v); //management_chain_level_04
        cto.u_management_chain_level_05 = getSysID(rowsRaxData[x].f[19].v); //management_chain_level_05
        cto.u_management_chain_level_06 = getSysID(rowsRaxData[x].f[20].v); //management_chain_level_06
        cto.u_management_chain_level_07 = getSysID(rowsRaxData[x].f[21].v); //management_chain_level_07
        cto.u_management_chain_level_08 = getSysID(rowsRaxData[x].f[22].v); //management_chain_level_08
        cto.u_management_chain_level_09 = getSysID(rowsRaxData[x].f[23].v); //management_chain_level_09
        cto.update();
    }

    // 	If duplicate records are not discovered, then data in CTO org table will be inserted. 
    else {
        var stagingTable = new GlideRecord('u_cto_org_data');
        stagingTable.initialize();
        stagingTable.u_email_id = rowsRaxData[x].f[0].v; //worker_email 
        stagingTable.u_first_name = rowsRaxData[x].f[1].v; //first_name 
        stagingTable.u_last_name = rowsRaxData[x].f[2].v; //last_name 
        stagingTable.u_name = rowsRaxData[x].f[3].v; //preferred_name 
        stagingTable.u_racker_name = getSysID(rowsRaxData[x].f[3].v);
        stagingTable.u_manager = getSysID(rowsRaxData[x].f[4].v); //work_manager
        stagingTable.u_user_id = rowsRaxData[x].f[5].v; //sso
        stagingTable.u_job_title = rowsRaxData[x].f[6].v; //job_profile
        stagingTable.u_channel_1 = rowsRaxData[x].f[7].v; //cch_l1
        stagingTable.u_channel_2 = rowsRaxData[x].f[8].v; //cch_l2
        stagingTable.u_channel_3 = rowsRaxData[x].f[9].v; //cch_l3
        stagingTable.u_channel_4 = rowsRaxData[x].f[10].v; //cch_l4
        stagingTable.u_channel_5 = rowsRaxData[x].f[11].v; //cch_l5
        stagingTable.u_channel_6 = rowsRaxData[x].f[12].v; //cch_l6
        stagingTable.u_channel_7 = rowsRaxData[x].f[13].v; //cch_l7
        stagingTable.u_management_chain_level_00 = getSysID(rowsRaxData[x].f[14].v); //management_chain_level_00
        stagingTable.u_management_chain_level_01 = getSysID(rowsRaxData[x].f[15].v); //management_chain_level_01
        stagingTable.u_management_chain_level_02 = getSysID(rowsRaxData[x].f[16].v); //management_chain_level_02
        stagingTable.u_management_chain_level_03 = getSysID(rowsRaxData[x].f[17].v); //management_chain_level_03
        stagingTable.u_management_chain_level_04 = getSysID(rowsRaxData[x].f[18].v); //management_chain_level_04
        stagingTable.u_management_chain_level_05 = getSysID(rowsRaxData[x].f[19].v); //management_chain_level_05
        stagingTable.u_management_chain_level_06 = getSysID(rowsRaxData[x].f[20].v); //management_chain_level_06
        stagingTable.u_management_chain_level_07 = getSysID(rowsRaxData[x].f[21].v); //management_chain_level_07
        stagingTable.u_management_chain_level_08 = getSysID(rowsRaxData[x].f[22].v); //management_chain_level_08
        stagingTable.u_management_chain_level_09 = getSysID(rowsRaxData[x].f[23].v); //management_chain_level_09
        stagingTable.insert();
    }
}
