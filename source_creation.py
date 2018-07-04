from database import DataBase
import argparse
import yaml

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="File with properties")
    settings = parser.parse_args()

    webApi = DataBase()

    with open(settings.file, 'r') as ymlfile:
        config = yaml.load(ymlfile)
    databaseConfig = config.get('database')
    host = databaseConfig.get('host', "localhost")
    port = databaseConfig.get('port', "5432")
    user = databaseConfig.get('username', "postgres")
    password = databaseConfig.get('password', "admin")
    createDb = databaseConfig.get('auto-update', "no")
    waDB = databaseConfig.get('webapidb', "OHDSI")
    #TODO: Add possibility to name users
    webApiSchema = databaseConfig.get('webapi-schema', "webapi").lower().strip().replace(" ", "_")

    webApi.createConnection(waDB, user, password, host, port)

    sourceId = 0
    sourceDaimonId = 0
    oldDaimonId = 0
    connectionString = "jdbc:postgresql://{}:{}/{}?user={}&password={}".format(host, port, waDB, "ohdsi_admin_user", "admin1")

    sources = config.get('sources',[])

    if not settings.file:
        sourceName = input('Enter the source name (enter to skip): ')
    else:
        if len(sources) == sourceId:
            sourceName = ""
        else:
            sourceName = sources[sourceId].get("name")
    while sourceName != "":
        sourceKey = sourceName.strip().replace(" ", "_").upper()

        webApi.executeCommand('''INSERT INTO {0}.source
                                    (source_id, source_name, source_key, source_connection, source_dialect) VALUES
                                    ({1}, '{2}', '{3}', '{4}', 'postgresql');'''
                                    .format(webApiSchema, sourceId, sourceName, sourceKey, connectionString))

        daimons = sources[sourceId].get("daimons", [])

        if not settings.file:
            daimonSourceTable = input('Enter the daimon source table ({}) (enter to skip): '.format(webApiSchema))
        else:
            if oldDaimonId + len(daimons) == sourceDaimonId:
                daimonSourceTable = ""
            else:
                daimonSourceTable = daimons[sourceDaimonId - oldDaimonId].get("table")
        while daimonSourceTable != "":
            daimonSourceTable = webApiSchema if daimonSourceTable == "" else daimonSourceTable.strip().lower().replace(" ", "_")

            if not settings.file:
                d = input('Enter daimon type (CDM, Vocabulary, Results, Evidence): ')
            else:
                d = daimons[sourceDaimonId - oldDaimonId].get("type")

            if d.lower() == "cdm":
                daimonType = 0
            if d.lower() == "vocabulary":
                daimonType = 1
            if d.lower() == "results":
                daimonType = 2
            if d.lower() == "evidence":
                daimonType = 3

            if not settings.file:
                priority = input('Enter daimon priority (0): ')
                priority = 0 if priority == "" else int(priority)
            else:
                priority = daimons[sourceDaimonId - oldDaimonId].get("priority", 0)
            webApi.executeCommand('''INSERT INTO {0}.source_daimon
                                        (source_daimon_id, source_id, daimon_type, table_qualifier, priority) VALUES
                                        ({1},{2},{3}, '{4}', {5});'''.format(webApiSchema, sourceDaimonId, sourceId, daimonType, daimonSourceTable, priority))
            sourceDaimonId += 1
            if not settings.file:
                daimonSourceTable = input('Enter the daimon source table ({}) (enter to skip): '.format(webApiSchema))
            else:
                if oldDaimonId + len(daimons) == sourceDaimonId:
                    daimonSourceTable = ""
                else:
                    daimonSourceTable = daimons[sourceDaimonId - oldDaimonId].get("table")
        sourceId += 1
        oldDaimonId = sourceDaimonId

        if not settings.file:
            sourceName = input('Enter the source name (enter to skip): ')
        else:
            if len(sources) == sourceId:
                sourceName = ""
            else:
                sourceName = sources[sourceId].get("name")
