import json
import MySQLdb as mdb
import os


class lrmi_ETL_insert:
    def __init__(self, directory, host="localhost", user="testuser", password="Rhyno", database="testdb"):
        self.dir = directory
        self.mysql = mdb.connect(host, user, password, database)

    def run(self):
        """Manages other functions"""
        os.chdir(self.dir)
        files = os.listdir(os.getcwd())
        for fi in files:
            data = self.j_read(fi)
            f_data = self.fields(data)
            self.insert(f_data)

    def j_read(self, fi):
        """Reads from the JSON files"""
        f = open(fi)
        jdata = json.load(f)
        f.close()
        return jdata

    def fields(self, j):
        """Extracts the fields from the JSON file"""
        temp = {
            "doc_ID":"EXTERNAL_GUID",
            "name":"NAME",
            "url":"URL",
            "description":"DESCRIPTION",
            "copyrightYear":"COPYRIGHT_YEAR",
            "useRightsUrl":"USE_RIGHTS_URL",
            "isBasedOnUrl":"IS_BASED_ON_URL",
            "timeRequired":"TIME_REQUIRED",
            "inLanguage":"LANGUAGE_ID",
            "interactivityType":"INTERACTIVITY_ID",
            "learningResourceType":"LEARNING_RESOURCE_ID",
            "dateCreated":"DATE_CREATED",
            "dateModified":"DATE_MODIFIED"}
        data = {}
        data['doc_ID'] = str(j['doc_ID'])
        data['name'] = str(j['resource_data_description']['resource_data']['items'][0]['properties']['name'][0])
        data['url'] = str(j['resource_data_description']['resource_data']['items'][0]['properties']['url'][0])
        data['description'] = str(j['resource_data_description']['resource_data']['items'][0]['properties']['description'][0])
        data['dateCreated'] = str(j['resource_data_description']['resource_data']['items'][0]['properties']['dateCreated'][0])
        data['inLanguage'] = str(j['resource_data_description']['resource_data']['items'][0]['properties']['inLanguage'][0])
        f_data = {}
        for key in data:
            f_data[temp[key]] = data[key]
        return f_data

    def insert(self, f_data):
        """Inserts the fields into their appropriate fields in the database"""
        update = False
        with self.mysql as m:
            m.execute("SELECT * FROM resources")
            rows = m.fetchall()
            for row in rows:
                print row
                if f_data['EXTERNAL_GUID'] in row:
                    print "?"
                    update = True
            if update:
                for key in f_data:
                    if key != "EXTERNAL_GUID":
                        m.execute("UPDATE resources SET %s = %s WHERE %s = %s", (key, f_data[key], 'EXTERNAL_GUID',  f_data['EXTERNAL_GUID']))
            else:
                keys = []
                values = []
                for key in f_data.keys():
                    keys.append("'" + key + "', ")
                for val in f_data.values():
                    values.append("'" + val + "', ")
                print keys
                print values
                m.execute("INSERT INTO resources(%s) VALUES(%s)", (keys, values))
            m.close()
if __name__ == "__main__":
    insert = lrmi_ETL_insert("C:\\users\user\\Desktop\\test")
    insert.run()
                
                    

            
            
            
            
        
