import json
import MySQLdb as mdb
import os
import sys
import glob
import logging
import datetime
from dateutil.parser import parse

logging.basicConfig(filename='example.log',level=logging.DEBUG)


class lrmi_ETL_insert:
    def __init__(self, directory, host="localhost", user="lri", password="lri", database="lri"):
        self.dir = directory
        self.mysql = mdb.connect(host, user, password, database)
        self.mysql.set_character_set('utf8')

    def run(self):
        """Manages other functions"""
        os.chdir(self.dir)
        files = glob.glob(os.getcwd() + "/*.json")

        for fi in files:
            data = self.j_read(fi)
            resource = self.fields(data)

            if resource != {}:
                self.insert(resource)

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

    def fmt(self, value):
        if value == None:
            return "NULL"
        elif type(value).__name__ == "datetime":
            return "'" + value.strftime("%Y-%m-%d") + "'"
        else:
            return "'" + value.replace(u"'",u"''") + "'"

    def date_format(self, value):
        if value != None:
            try:
                value = parse(value)
            except:
                value = None
                pass
        return value


    def fields(self, j):
        """Extracts the fields from the JSON file"""
        field_map = {
            "doc_ID":"EXTERNAL_GUID",
            "name":"NAME",
            "url":"URL",
            "description":"DESCRIPTION",
            "copyrightYear":"COPYRIGHT_YEAR",
            "useRightsUrl":"USE_RIGHTS_URL",
            "isBasedOnUrl":"IS_BASED_ON_URL",
            "timeRequired":"TIME_REQUIRED",
            "dateCreated":"DATE_CREATED",
            "datePublished":"DATE_PUBLISHED",
            "dateModified":"DATE_MODIFIED",
            "sourceText":"SOURCE_TEXT"}
        resource = {}
        resource_alignments = []

        try:
            #self.print_keys(j)

            if 'resource_data_description' in j:
                if 'resource_data' in j['resource_data_description']:
                    if 'items' in j['resource_data_description']['resource_data'] and len(j['resource_data_description']['resource_data']['items']) > 0:
                        item = j['resource_data_description']['resource_data']['items'][0]
                        if 'properties' in item:
                            resource['doc_ID'] = str(j['doc_ID'])
                            doc_id = str(j['doc_ID'])

                            properties = item['properties']

                            resource['name'] = self.return_value(properties, 'name')
                            if resource['name'] == None:
                                resource['name'] = '(Unknown)'
                            resource['url'] = self.return_value(properties, 'url')
                            resource['description'] = self.return_value(properties, 'description')
                            resource['useRightsUrl'] = self.return_value(properties, 'useRightsUrl')
                            resource['isBasedOnUrl'] = self.return_value(properties, 'isBasedOnUrl')
                            resource['copyrightYear'] = self.return_value(properties, 'copyrightYear')
                            ##resource['inLanguage'] = self.return_value(properties, 'inLanguage')
                            resource['dateCreated'] = self.date_format(self.return_value(properties, 'dateCreated'))
                            resource['datePublished'] = self.date_format(self.return_value(properties, 'datePublished'))
                            resource['dateModified'] = self.date_format(self.return_value(properties, 'dateModified'))
                            resource['sourceText'] = json.dumps(j, indent=2);

                            if 'educationalAlignment' in properties and type(properties['educationalAlignment']) == list:
                                ##print type ( properties['educationalAlignment'] ).__name__

                                for alignment in properties['educationalAlignment']:
                                    if 'type' in alignment and len(alignment['type']) > 0 and alignment['type'][0] == "http://schema.org/AlignmentObject":
                                        if 'properties' in alignment:
                                            if 'educationalFramework' in alignment['properties']:
                                                resource_alignment = {}
                                                if 'educationalFramework' in alignment['properties'] and len(alignment['properties']['educationalFramework']) > 0 and alignment['properties']['educationalFramework'][0] != "":
                                                    resource_alignment['educationalFramework'] = alignment['properties']['educationalFramework'][0]
                                                else:
                                                    resource_alignment['educationalFramework'] = "Unknown"

                                                if 'targetName' in alignment['properties'] and len(alignment['properties']['targetName']) > 0 and alignment['properties']['targetName'][0] != "":
                                                    resource_alignment['targetName'] = alignment['properties']['targetName'][0]
                                                else:
                                                    resource_alignment['targetName'] = "Unknown"

                                                if 'alignmentType' in alignment['properties'] and len(alignment['properties']['alignmentType']) > 0 and alignment['properties']['alignmentType'][0] != "":
                                                    resource_alignment['alignmentType'] = alignment['properties']['alignmentType'][0]
                                                else:
                                                    resource_alignment['alignmentType'] = "Unknown"


                                                resource_alignments.append(resource_alignment)

                            ##resource['thumbnailUrl'] = self.return_value(properties, 'thumbnailUrl')
                            ##resource['publisher'] = self.return_value(properties, 'publisher')
                            ##resource['about'] = self.return_value(properties, 'publisher')

                            """useRightsUrl
                            isBasedOnUrl
                            learningResourceType
                            interactivityType
                            typicalAgeRange
                            timeRequired
                            educationalUse
                            educationalAlignment
                            educationalRole"""

                            ##if resource['inLanguage'] != None:
                            ##    print json.dumps(resource, indent=2)

                """
                if 'identity' in j['resource_data_description']:
                    print j['resource_data_description']['identity']['submitter']
                    if j['resource_data_description']['identity']['submitter'] == "inBloom Tagger Application <tagger@inbloom.org>":
                        print j['resource_data_description']['identity']
                """
        except Exception as ex:
            print "Exception in field extraction: " + str(ex.message)
            pass
        

        field_data = {}
        for key in resource:
            field_data[field_map[key]] = resource[key]
        if len(field_data) > 0:
            field_data['educationalAlignments'] = resource_alignments
        return field_data

    def insert(self, resource):
        """Inserts the fields into their appropriate fields in the database"""
        update = False
        with self.mysql as m:
            m.execute('SET NAMES utf8;')
            m.execute('SET CHARACTER SET utf8;')
            m.execute('SET character_set_connection=utf8;')
            m.execute("SELECT ID, EXTERNAL_GUID FROM resources WHERE EXTERNAL_GUID = %s", resource['EXTERNAL_GUID'])

            rows = m.fetchall()
            for row in rows:
                if resource['EXTERNAL_GUID'] in row:
                    update = True
            if update:
                pass
                #for key in resource:
                #    if key != "EXTERNAL_GUID":
                #        pass
                #        ##m.execute("UPDATE resources SET %s = %s WHERE %s = %s", (key, resource[key], 'EXTERNAL_GUID',  resource['EXTERNAL_GUID']))
            else:
                sql = "INSERT INTO resources (EXTERNAL_GUID, NAME, DESCRIPTION, URL, COPYRIGHT_YEAR, USE_RIGHTS_URL, IS_BASED_ON_URL, DATE_CREATED, DATE_PUBLISHED, SOURCE_TEXT) VALUES "
                sql = sql + "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                sql = sql % (self.fmt(resource['EXTERNAL_GUID']), self.fmt(resource['NAME']), self.fmt(resource['DESCRIPTION']), self.fmt(resource['URL']), self.fmt(resource['COPYRIGHT_YEAR']), \
                    self.fmt(resource['USE_RIGHTS_URL']), self.fmt(resource['IS_BASED_ON_URL']), self.fmt(resource['DATE_CREATED']), self.fmt(resource['DATE_PUBLISHED']), self.fmt(resource['SOURCE_TEXT']))

                m.execute(sql)
                resource_id = m.lastrowid

                if len(resource['educationalAlignments']) > 0:
                    for alignment in  resource['educationalAlignments']:
                        
                        framework_id = 0
                        m.execute("SELECT ID, SYNONYM_ID FROM frameworks WHERE NAME = %s", alignment['educationalFramework'])
                        framework = m.fetchone()
                        if framework is not None:
                            if framework[1] is not None:
                                framework_id = framework[1]
                            else:
                                framework_id = framework[0]
                        else:
                            m.execute("INSERT INTO frameworks (NAME, URL) VALUES (%s, '')", alignment['educationalFramework'])
                            framework_id = m.lastrowid

                        standard_id = 0
                        m.execute("SELECT ID FROM standards WHERE EXTERNAL_ID = %s AND FRAMEWORK_ID = %s", (alignment['targetName'], framework_id))
                        standard = m.fetchone()
                        if standard is not None:
                            standard_id = standard[0]
                        else:
                            m.execute("INSERT INTO standards (EXTERNAL_ID, FRAMEWORK_ID) VALUES (%s, %s)", (alignment['targetName'], framework_id))
                            standard_id = m.lastrowid

                        alignment_type_id = 0
                        m.execute("SELECT ID FROM alignment_types WHERE NAME = %s", alignment['alignmentType'])
                        alignment_type = m.fetchone()
                        if alignment_type is not None:
                            alignment_type_id = alignment_type[0]
                        else:
                            m.execute("INSERT INTO alignment_types (NAME) VALUES (%s)", alignment['alignmentType'])
                            alignment_type_id = m.lastrowid

                        if standard_id > 0 and alignment_type_id > 0:
                            m.execute("INSERT INTO alignments (RESOURCE_ID, STANDARD_ID, ALIGNMENT_TYPE_ID) VALUES (%s, %s, %s)", (resource_id, standard_id, alignment_type_id))

            m.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        insert = lrmi_ETL_insert(str(sys.argv[1]))
        insert.run()
    else:
        print "Please provide a directory."


                
                    

            
            
            
            
        
