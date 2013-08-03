import json
import MySQLdb as mdb
import os
import sys
import glob
import logging

logging.basicConfig(filename='example.log',level=logging.DEBUG)


class lrmi_ETL_insert:
    def __init__(self, directory, host="localhost", user="lri", password="lri", database="lri"):
        self.dir = directory
        self.mysql = mdb.connect(host, user, password, database)

    def run(self):
        """Manages other functions"""
        os.chdir(self.dir)
        ##files = os.listdir(os.getcwd())
        files = glob.glob(os.getcwd() + "/*.json")

        for fi in files:
            data = self.j_read(fi)
            f_data = self.fields(data)
            ## print f_data
            ### self.insert(f_data)

    def j_read(self, fi):
        """Reads from the JSON files"""
        ## print "Processing: " + fi
        f = open(fi)
        jdata = ""
        try:
            jdata = json.load(f)
            f.close()
        except Exception as ex:
            print ex.message
            pass
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

        try:
            data['doc_ID'] = str(j['doc_ID'])
            doc_id = str(j['doc_ID'])

            if 'resource_data_description' in j:
                if 'resource_data' in j['resource_data_description']:
                    if 'items' in j['resource_data_description']['resource_data'] and len(j['resource_data_description']['resource_data']['items']) > 0:
                        item = j['resource_data_description']['resource_data']['items'][0]
                        if 'properties' in item:
                            properties = item['properties']

                            if 'name' in properties:
                                if type(properties['name']) == list and len(properties['name'])>0:
                                    data['name'] = properties['name'][0]
                                else:
                                    logging.warning(doc_id + " name not found")
                            else:
                                logging.warning(doc_id + " name not found")

                            if 'url' in properties:
                                if type(properties['url']) == list and len(properties['url'])>0:
                                    data['url'] = properties['url'][0]
                                else:
                                    logging.warning(doc_id + " URL not found")
                            else:
                                logging.warning(doc_id + " URL not found")

                            if 'description' in properties:
                                if type(properties['description']) == list and len(properties['description'])>0:
                                    data['description'] = properties['description'][0]
                                else:
                                    logging.warning(doc_id + " description not found")
                            else:
                                logging.warning(doc_id + " description not found")

                if 'identity' in j['resource_data_description']:
                    print j['resource_data_description']['identity']['submitter']
                    if j['resource_data_description']['identity']['submitter'] == "inBloom Tagger Application <tagger@inbloom.org>":
                        print j['resource_data_description']['identity']



            ##print data

            ##data['name'] = str(j['resource_data_description']['resource_data']['items'][0]['properties']['name'][0])
            #data['url'] = str(j['resource_data_description']['resource_data']['items'][0]['properties']['url'][0])
            #data['description'] = str(j['resource_data_description']['resource_data']['items'][0]['properties']['description'][0])
            #data['dateCreated'] = str(j['resource_data_description']['resource_data']['items'][0]['properties']['dateCreated'][0])
            #data['inLanguage'] = str(j['resource_data_description']['resource_data']['items'][0]['properties']['inLanguage'][0])
        except Exception as ex:
            print "Exception in field extraction: " + str(ex.message)
            pass
        

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
    if len(sys.argv) > 1:
        insert = lrmi_ETL_insert(str(sys.argv[1]))
        insert.run()
    else:
        print "Please provide a directory."


                
                    

            
            
            
            
        
