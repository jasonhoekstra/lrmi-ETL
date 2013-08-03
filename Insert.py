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
        files = glob.glob(os.getcwd() + "/*.json")

        for fi in files:
            data = self.j_read(fi)
            resource = self.fields(data)
            ##self.insert(resource)

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


    def print_keys(self, element):
        if type(element) == dict:
            for key in element.keys():
                print key
                self.print_keys(element[key])
        elif type(element) == list:
            for item in element:
                self.print_keys(item)

    def return_value(self, haystack, needle):
        return_data = None
        
        if needle in haystack:
            if type(haystack[needle]) == list and len(haystack[needle])>0:
                return_data = haystack[needle][0]

        return return_data

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
        resource = {}

        try:
            resource['doc_ID'] = str(j['doc_ID'])
            doc_id = str(j['doc_ID'])

            #self.print_keys(j)

            if 'resource_data_description' in j:
                if 'resource_data' in j['resource_data_description']:
                    if 'items' in j['resource_data_description']['resource_data'] and len(j['resource_data_description']['resource_data']['items']) > 0:
                        item = j['resource_data_description']['resource_data']['items'][0]
                        if 'properties' in item:
                            properties = item['properties']

                            resource['name'] = self.return_value(properties, 'name')
                            if resource['name'] == None:
                                resource['name'] = '(Unknown)'
                            resource['url'] = self.return_value(properties, 'url')
                            resource['description'] = self.return_value(properties, 'description')
                            resource['inLanguage'] = self.return_value(properties, 'inLanguage')
                            resource['dateCreated'] = self.return_value(properties, 'dateCreated')
                            resource['dateModified'] = self.return_value(properties, 'dateModified')
                            resource['datePublished'] = self.return_value(properties, 'datePublished')
                            ##resource['thumbnailUrl'] = self.return_value(properties, 'thumbnailUrl')
                            resource['publisher'] = self.return_value(properties, 'publisher')
                            resource['about'] = self.return_value(properties, 'publisher')
                            resource['useRightsUrl'] = self.return_value(properties, 'useRightsUrl')
                            resource['isBasedOnUrl'] = self.return_value(properties, 'isBasedOnUrl')


                            """useRightsUrl
                            isBasedOnUrl
                            learningResourceType
                            interactivityType
                            typicalAgeRange
                            timeRequired
                            educationalUse
                            educationalAlignment
                            educationalRole"""


                            if resource['inLanguage'] != None:
                                print json.dumps(resource, indent=2)


                """
                if 'identity' in j['resource_data_description']:
                    print j['resource_data_description']['identity']['submitter']
                    if j['resource_data_description']['identity']['submitter'] == "inBloom Tagger Application <tagger@inbloom.org>":
                        print j['resource_data_description']['identity']
                """



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
        #for key in resource:
        #    f_data[temp[key]] = resource[key]
        return f_data

    def insert(self, resource):
        """Inserts the fields into their appropriate fields in the database"""
        update = False
        with self.mysql as m:
            m.execute("SELECT * FROM resources")
            rows = m.fetchall()
            for row in rows:
                print row
                if resource['EXTERNAL_GUID'] in row:
                    print "?"
                    update = True
            if update:
                for key in resource:
                    if key != "EXTERNAL_GUID":
                        m.execute("UPDATE resources SET %s = %s WHERE %s = %s", (key, resource[key], 'EXTERNAL_GUID',  resource['EXTERNAL_GUID']))
            else:
                keys = []
                values = []
                for key in resource.keys():
                    keys.append("'" + key + "', ")
                for val in resource.values():
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


                
                    

            
            
            
            
        
