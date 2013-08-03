"""
***IMPORTANT***
Each time the program is run, you MUST sumbit the initial resumption token by system argument.


**Purpose

The purpose of this program is to extract the large JSON files from node01.public.learningregistry.net and divide
them into smaller files by document id. These smaller files will then be parsed and placed into a database by
parser.py for further use.

**ProgramStructure

*class lrmi_ETL_extract

The lrmi_ETL_extract classes gets the resumption token from the system argument. When the object for the class is
created, it creates the base urllib2 object, and the baseurl for the extraction.

*function (method) run

This is going to the main function for the class. This will run the whole process. First, it will open the url,
then it will download the file from the website (using the extract function). Next, it will open that file,and
devide each individual doc_id into files with the filenames as the doc_id (divide function). Then finally, it
will loop around and start the process again with the next url. When the function loops, it adds the past
resumption token to the self.resumption_tokens variable and assigns the new resumption token to self.token.

*function (method) setup

Sets up the directories needed for the program to run.

*function (method) extract

This function will formulate the url and download the base JSON file.

*function (method) divide

This function will extract each document(object with the 'doc_ID' string) from the original JSON file and place
them into individual files by their doc_id.

*function (method) build_url

This function will build the proper url based off of the resumption_token and baseurl.

*function (method) result_count

This function retrieves the resultCount from the original JSON file.

*function (method) resumption_token

This function retrieves the resumption token from the original JSON files.

*function (method) report

This function reports all the data that has been processed and writes the resumption token index.

"""

import urllib as u
import sys
import json
import time
import os
import re



class lrmi_ETL_extract:
    def __init__(self, token = ""):
        if token != "":
            self.base_url = "https://node01.public.learningregistry.net/slice?any_tags=lrmi&resumption_token={0}"
            self.base_url_token = "https://node01.public.learningregistry.net/slice?any_tags=lrmi&resumption_token={0}"            
        else:
            self.base_url = "https://node01.public.learningregistry.net/slice?any_tags=lrmi"
            self.base_url_token = "https://node01.public.learningregistry.net/slice?any_tags=lrmi&resumption_token={0}"
        self.token = token
        self.resumption_tokens = []
        self.resultCount = None
        self.resultCountOrig = None
        self.date = time.strftime("%m-%d-%Y")
        self.dir = None
        self.extract_loop = 0
        
    def run(self):
        """This function manages the other functions."""
        self.setup()
        print "Starting the extractoin process"
        url = self.build_url()
        filename = self.extract(url)
        self.resumption_token(filename)
        self.result_count(filename, True)
        self.divide(self.dir+".json")
        while True:
            url = self.build_url()
            filename = self.extract(url)
            self.result_count(filename)
            if self.resultCount == 1:
                break
            self.divide(self.dir+".json")
            self.resumption_token(filename)
            self.extract_loop = 0
        self.report()

    def setup(self):
        print "Creating necessary files..."
        self.dir = os.getcwd()+ "/lrmi_ETL/".format(str(self.date))
        os.mkdir(self.dir)
        os.chdir(self.dir)
        os.mkdir("Documents")
        
    def extract(self, url):
        """This function will download the base JSON file.
        Arguments:
        url  --  type: string, description: the url the json file is at."""
        while True:
            try:
                if os.path.exists(self.dir+".json"):
                    os.remove(self.dir+".json")
                u.URLopener().retrieve(url, self.dir+".json")
                fi = open(self.dir+".json")
                data = fi.read()
                fi.close()
                if "resultCount" not in data:
                    time.sleep(2)
                    if self.extract_loop ==10:
                        raise e(error)
                    self.extract_loop+=1
                    continue
                return self.dir+".json"
            except IOError:
                print "Bad Gateway - retrying"
                time.sleep(2)
                self.extract_loop +=1
                if self.extract_loop ==10:
                    raise Exception("Couldn't not access the JSON file")
        
        
    def divide(self, filename):
        """This function will divide each document(object with the 'doc_ID' string) from the original JSON file and place them into individual files by their doc_id.
	Arguments:
	filename  --  type: str, description: filename of the file you want to divide by doc_ID objects."""
        f = open(filename)
        try:
            j = json.load(f)
            if os.getcwd() != self.dir+"/Documents":
                os.chdir(self.dir+"/Documents")
            for  x in j['documents']:
                fi = open(str(x['doc_ID'])+".json", "w")
                fi.write(json.dumps(x, indent=2))
                fi.close()
            f.close()
        except ValueError:
            raw_input("Couldn't parse file due to JSON errors")
            exit(1)
        

    def build_url(self):
        """This function will build the proper url based off of the resumption_token and baseurl."""
        url = self.base_url.format(self.token)
        self.base_url = self.base_url_token
        return url

    def result_count(self, filename, original=False):
        """This function retrieves the resultCount from the original JSON file.
        Arguments:
        filename  --  type: str, description: filename of the file from which you want to extract the result count"""
        f = open(filename, "r")
        j = json.load(f)
        f.close()
        if original:
            self.resultCountOrig = j['resultCount']
        self.resultCount = j['resultCount']
        return True
        
    def resumption_token(self, filename):
        """This function retrieves the resumption token from the original JSON files.
        Arguments:
        filename  --  type: str, description: filename of the file from which you want to extract the resumption_token"""
        self.resumption_tokens.append(self.token)
        f = open(filename, "r")
        j = json.load(f)
        f.close()
        self.token = j['resumption_token']
        return True
        
    def report(self):
        """This function reports all the data that has been processed and writes the resumption token index."""
        f = open("{0} resumption tokens.dat".format(self.date), "w")
        f.write("\n".join(self.resumption_tokens))
        f.close()
        print "Started at {0} and ended at {1}.\nDirectory: '{3}'\nTotal files: {2}\nIndex of resumption tokens: '{0} resumption tokens.json'".format(self.date, time.strftime("%m/%d/%Y %I:%M"), self.resultCountOrig, self.dir)   
        raw_input("Press <Enter> to exit")
        exit()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        e = lrmi_ETL_extract(str(sys.argv[1]))
    else:
        e = lrmi_ETL_extract("")
    e.run()
        
