from database import DataBase
import argparse
import yaml

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", help="File with properties", action="store")
    settings = parser.parse_args()

    if not settings.file:
        host = input("Enter host (localhost): ")
        host = ("localhost" if host == '' else host)
        port = input("Enter port (5432): ")
        port = ("5432" if port == '' else port)
        user = input("Enter username (postgres): ")
        user = ('postgres' if user == '' else user)
        password = input("Enter password (admin): ")
        password = ('admin' if password == '' else password)
        createDb = input("Create database? (n)")
        if createDb.lower() == "y" or createDb.lower() == "yes" or createDb.lower() == 'true':
            dbName = input("Enter database name from which to create WebAPI (postgres): ")
            dbName = ('postgres' if dbName == '' else dbName)
        waDB = input("Enter WebAPI database name (OHDSI): ")
        waDB = ('OHDSI' if waDB == '' else waDB)
        createUsers = input("Create users? (n)")
        #TODO: Add possibility to name users
        webApiSchema = input("Enter webapi schema name (webapi): ")
        webApiSchema = ("webapi" if webApiSchema == '' else webApiSchema.lower().strip().replace(" ", "_"))
        resultsSchema = input("Enter results schema name (webapi): ")
        resultsSchema = ("webapi" if resultsSchema == '' else resultsSchema.lower().strip().replace(" ", "_"))
    else:
        with open(settings.file, 'r') as ymlfile:
            config = yaml.load(ymlfile)
        databaseConfig = config.get('database')

        host =  databaseConfig.get('host', "localhost")
        port =  databaseConfig.get('port', "5432")
        user =  databaseConfig.get('username', "postgres")
        password =  databaseConfig.get('password', "admin")
        createDb =  str(databaseConfig.get('create-database', "no"))
        if createDb.lower() == "y" or createDb.lower() == "yes" or createDb.lower() == "true":
            dbName =  databaseConfig.get('sourcedb', "postgres")
        waDB =  databaseConfig.get('webapidb', "OHDSI")
        createUsers =  str(databaseConfig.get('create-users', "no"))
        #TODO: Add possibility to name users
        webApiSchema =  databaseConfig.get('webapi-schema', "webapi").lower().strip().replace(" ", "_")
        resultsSchema =  databaseConfig.get('results-schema', "webapi").lower().strip().replace(" ", "_")

    webApi = DataBase()
    if createDb.lower() == "y" or createDb.lower() == "yes" or createDb.lower() == "true":
        webApi.createConnection(dbName, user, password, host, port)
    else:
        webApi.createConnection(waDB, user, password, host, port)

    if createUsers.lower() == 'y' or createUsers.lower() == 'yes' or createUsers.lower() == 'true':
        #REPLICATION could be added to ohdsi_admin role
        webApi.executeCommand('''CREATE ROLE ohdsi_admin
                                    CREATEDB
                                     VALID UNTIL 'infinity';
                                    COMMENT ON ROLE ohdsi_admin
                                    IS 'Administration group for OHDSI applications';''')
        webApi.executeCommand('''CREATE ROLE ohdsi_app
                                     VALID UNTIL 'infinity';
                                    COMMENT ON ROLE ohdsi_app
                                    IS 'Application groupfor OHDSI applications';''')
        webApi.executeCommand('''CREATE ROLE ohdsi_admin_user LOGIN ENCRYPTED PASSWORD 'md58d34c863380040dd6e1795bd088ff4a9'
                                     VALID UNTIL 'infinity';
                                    GRANT ohdsi_admin TO ohdsi_admin_user;
                                    COMMENT ON ROLE ohdsi_admin_user
                                    IS 'Admin user account for OHDSI applications';''')
        webApi.executeCommand('''CREATE ROLE ohdsi_app_user LOGIN ENCRYPTED PASSWORD 'md55cc9d81d14edce93a4630b7c885c6410'
                                     VALID UNTIL 'infinity';
                                    GRANT ohdsi_app TO ohdsi_app_user;
                                    COMMENT ON ROLE ohdsi_app_user
                                    IS 'Application user account for OHDSI applications';''')
        print('\n==> Users created <==\n')
    elif createUsers.lower() != '' and createUsers.lower() != 'n' and createUsers.lower() != 'no' and createUsers.lower() != 'false':
        print('\n==> Could not understand whether to create users or not, did not create them <==\n')
    webApi.close()
    webApi.createConnection(waDB, "ohdsi_admin_user", "admin1", host, port)
    if createDb.lower() == "y" or createDb.lower() == "yes" or createDb.lower() == 'true':
        webApi.executeCommand('''CREATE DATABASE "{0}"
                                    WITH ENCODING='UTF8'
                                         OWNER=ohdsi_admin
                                         CONNECTION LIMIT=-1;'''.format(waDB))
        webApi.executeCommand('''COMMENT ON DATABASE "{0}"
                                    IS 'OHDSI database';
                                    GRANT ALL ON DATABASE "{0}" TO GROUP ohdsi_admin;
                                    GRANT CONNECT, TEMPORARY ON DATABASE "{0}" TO GROUP ohdsi_app;'''.format(waDB))
        print('\n==> Database {} created <==\n'.format(waDB))
    webApi.executeCommand('''CREATE SCHEMA {0}
                                     AUTHORIZATION ohdsi_admin;
                                COMMENT ON SCHEMA {0}
                                IS 'Schema containing tables to support WebAPI functionality';
                                GRANT USAGE ON SCHEMA {0} TO public;
                                GRANT ALL ON SCHEMA {0} TO GROUP ohdsi_admin;
                                GRANT USAGE ON SCHEMA {0} TO GROUP ohdsi_app;'''.format(webApiSchema))
    print('\n==> {0} schema created <==\n'.format(webApiSchema))

    if resultsSchema != "webapi":
        webApi.executeCommand('''CREATE SCHEMA {0}
                                         AUTHORIZATION ohdsi_admin;
                                    COMMENT ON SCHEMA {0}
                                    IS 'Schema containing tables to support WebAPI functionality, the results tables';
                                    GRANT USAGE ON SCHEMA {0} TO public;
                                    GRANT ALL ON SCHEMA {0} TO GROUP ohdsi_admin;
                                    GRANT USAGE ON SCHEMA {0} TO GROUP ohdsi_app;'''.format(resultsSchema))
        print('\n==> {0} schema created <==\n'.format(resultsSchema))

    webApi.executeCommand('''ALTER DEFAULT PRIVILEGES IN SCHEMA {0}
                                  GRANT INSERT, SELECT, UPDATE, DELETE, REFERENCES, TRIGGER ON TABLES
                                  TO ohdsi_app;
                                ALTER DEFAULT PRIVILEGES IN SCHEMA {0}
                                  GRANT SELECT, USAGE ON SEQUENCES
                                  TO ohdsi_app;
                                ALTER DEFAULT PRIVILEGES IN SCHEMA {0}
                                  GRANT EXECUTE ON FUNCTIONS
                                  TO ohdsi_app;
                                ALTER DEFAULT PRIVILEGES IN SCHEMA {0}
                                  GRANT USAGE ON TYPES
                                  TO ohdsi_app;'''.format(webApiSchema))
    print('\n==> Priviliges adjusted <==\n')
    if resultsSchema != webApiSchema:
        webApi.executeCommand(open('additionalTables.sql','r').read().format(resultsSchema))
        print('\n==> Additional tables created in the results table, so Atlas can work with them <==\n'.format(resultsSchema))
