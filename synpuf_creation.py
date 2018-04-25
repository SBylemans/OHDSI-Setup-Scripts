from database import DataBase
import argparse
import yaml
import sys
import subprocess
import os
import platform

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="File with properties")
    settings = parser.parse_args()

    with open(settings.file, 'r') as ymlfile:
        config = yaml.load(ymlfile)
    databaseConfig = config.get('database')

    host =  databaseConfig.get('host', "localhost")
    port =  databaseConfig.get('port', "5432")
    user =  databaseConfig.get('username', "postgres")
    password =  databaseConfig.get('password', "admin")

    synpufDB = DataBase()
    synpufDB.createConnection(databaseConfig.get('webapidb', "OHDSI"), user, password, host, port)

    synpufConfig = config.get('synpuf')

    mainSchema = synpufConfig.get("main_schema", "synpuf5")
    achillesResultsSchema = synpufConfig.get("achilles_results_schema", "results")
    synpufDB.executeCommand('''CREATE SCHEMA {0}'''.format(mainSchema))
    synpufDB.executeCommand('''CREATE SCHEMA IF NOT EXISTS {0}'''.format(achillesResultsSchema))

    setupDir = synpufConfig.get("setup_dir", "SynPUF")
    vocDir = synpufConfig.get("vocabulary_dir", "voc_all")


    if platform.system() == "Windows":
        if not os.path.exists(os.getenv('APPDATA')+"\\postgresql\\"):
            os.makedirs(os.getenv('APPDATA')+"\\postgresql\\")
        with open(os.getenv('APPDATA')+"\\postgresql\\pgpass.conf", 'w') as passFile:
            passFile.write('{0}:{1}:{2}:{3}:{4}'.format(host,port,databaseConfig.get('webapidb', "OHDSI"),user, password))
    elif platform.system() == "Linux":
        with open(os.getenv('HOME')+"/.pgpass", 'w') as passFile:
            passFile.write('{0}:{1}:{2}:{3}:{4}'.format(host,port,databaseConfig.get('webapidb', "OHDSI"),user, password))



    with open(sys.path[0].replace('\\','/')+"/"+setupDir+"/create_CDMv5_tables.sql", 'r') as createTables:
        synpufDB.executeCommand(createTables.read().format(mainSchema))

    with open(sys.path[0].replace('\\','/')+"/"+setupDir+"/load_CDMv5_vocabulary.sql", 'r') as loadVoc:
        with open(sys.path[0].replace('\\','/')+"/"+setupDir+"/load_CDMv5_vocabulary_formatted.sql", "w") as formattedVoc:
            formattedVoc.write(loadVoc.read().format(mainSchema, sys.path[0].replace('\\','/')+"/"+setupDir+"/"+vocDir))
    subprocess.call("psql -h {0} -p {1} -U {2} -w -d {3} -f \"{4}\"".format(host, port, user, databaseConfig.get('webapidb', "OHDSI"), sys.path[0].replace('\\','/')+"/"+setupDir+"/load_CDMv5_vocabulary_formatted.sql"))

    with open(sys.path[0].replace('\\','/')+"/"+setupDir+"/load_CDMv5_synpuf.sql", 'r') as loadSyn:
        with open(sys.path[0].replace('\\','/')+"/"+setupDir+"/load_CDMv5_synpuf_formatted.sql", "w") as formattedSyn:
            formattedSyn.write(loadSyn.read().format(mainSchema, sys.path[0].replace('\\','/')+"/"+setupDir))
    subprocess.call("psql -h {0} -p {1} -U {2} -w -d {3} -f \"{4}\"".format(host, port, user, databaseConfig.get('webapidb', "OHDSI"), sys.path[0].replace('\\','/')+"/"+setupDir+"/load_CDMv5_synpuf_formatted.sql"))

    with open(sys.path[0].replace('\\','/')+"/"+setupDir+"/create_CDMv5_constraints.sql", 'r') as createConstraints:
        synpufDB.executeCommand(createConstraints.read().format(mainSchema))

    with open(sys.path[0].replace('\\','/')+"/"+setupDir+"/create_CDMv5_indices.sql", 'r') as createIndices:
        synpufDB.executeCommand(createIndices.read().format(mainSchema))

    with open(sys.path[0].replace('\\','/')+"/"+setupDir+"/create_CDMv5_condition_era.sql", 'r') as createConditionEra:
        synpufDB.executeCommand(createConditionEra.read().format(mainSchema))

    with open(sys.path[0].replace('\\','/')+"/"+setupDir+"/create_CDMv5_drug_era_non_stockpile.sql", 'r') as createDrugEra:
        synpufDB.executeCommand(createDrugEra.read().format(mainSchema))
