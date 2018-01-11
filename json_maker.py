import json
import argparse
import sqlite3
import gzip
import codecs
import os
import time

# TODO: Make class that creates metadata.json

# [x[0] for x in curs.fetchall()] Creates a list from sqlite query
#           WHERE only one column is returned

class JsonMaker:

    def __init__(self):
        self.JSON_LIMIT = 1000
        self.seconds = time.time()

    def getFeatures(self, in_d):
        features = []
        with gzip.open('/'.join([in_d, 'data.tsv.gz']), 'r') as in_f:
            features = codecs.decode(in_f.readline()).rstrip().split('\t')[1:]
        return features

    def getSampleIDs(self, out_d):
        conn = sqlite3.connect('/'.join([out_d, 'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT DISTINCT sampleName
                            FROM sampleTable''')

        return [x[0] for x in curs.fetchall()]

    def countSampleIDs(self, out_d):
        conn = sqlite3.connect('/'.join([out_d, 'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT count(DISTINCT sampleName)
                            FROM sampleTable''')

        return curs.fetchone()[0]

    def getUnique(self, in_d, variable):
        query = '''SELECT DISTINCT value 
                    FROM tempTable 
                    WHERE variable = "''' + variable + '"'

        conn = sqlite3.connect('/'.join([in_d, 'temp.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute(query)

        return [x[0] for x in curs.fetchall()]

    def countUnique(self, in_d, variable):
        query = '''SELECT count(DISTINCT value) 
                    FROM tempTable 
                    WHERE variable = "''' + variable + '"'

        conn = sqlite3.connect('/'.join([in_d, 'temp.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute(query)

        return curs.fetchone()[0]

    def putOne(self, in_d, out_d, variable):
        """
            Puts values of single variable into sqlite variableTable instead of metadata.json
        """
        conn = sqlite3.connect('/'.join([out_d, 'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            unique_vals = self.getUnique(in_d, variable)
            curs.execute('''UPDATE
                        SET numOptions = ''' + str(len(unique_vals)) + ''',
                        options = "''' + ','.join(unique_vals) + '''"
                        WHERE variableName = "''' + variable + '"')

    def putAll(self, in_d, out_d, typesDict):
        """
            Puts values of all variables into sqlite variableTable instead of metadata.json
            NOTE: if >JSON_LIMIT values, set to "null"
        """
        conn = sqlite3.connect('/'.join([out_d, 'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            for variable in typesDict:
                cur_type = typesDict[variable]
                
                if cur_type == "integer":
                    unique_vals = self.getUnique(in_d, variable)
                    curs.execute('''UPDATE
                                SET numOptions = ''' + 
                                str(len(unique_vals)) + ''',
                                    options = "''' +
                                min(unique_vals) + ',' + max(unique_vals) +
                                 '''WHERE variableName = "''' +
                                variable + '"')
                elif cur_type == "real":
                    unique_vals = self.getUnique(in_d, variable)
                    curs.execute('''UPDATE
                                SET numOptions = ''' + 
                                str(len(unique_vals)) + ''',
                                    options = "''' +
                                min(unique_vals) + ',' + max(unique_vals) +
                                '''WHERE variableName = "''' +
                                variable + '"')
                else:
                    unique_count = self.countUnique(in_d, variable)
                    if unique_count < self.JSON_LIMIT:
                        unique_vals = self.getUnique(in_d, variable)
                        curs.execute('''UPDATE
                                    SET numOptions = ''' + str(len(unique_vals)) + ''',
                                    options = "''' + ','.join(unique_vals) + '''"
                                    WHERE variableName = "''' + variable + '"')
                    else:
                        curs.execute('''UPDATE
                                    SET numOptions = ''' + str(len(curSet)) + ''',
                                    options = "null"
                                    WHERE variableName = "''' + variable + '"')
        
    def createJson(self, in_d, out_d, typesDict):
        features = self.getFeatures(in_d)
        jsonDict = None
        if len(features) < self.JSON_LIMIT:
            jsonDict = {"features": {"numOptions": len(features),
                        "options": features},
                        "meta": {}}
        else:
            jsonDict = {"features": {"numOptions": len(features),
                        "options": None},
                        "meta": {}}
        if len(typesDict) < self.JSON_LIMIT:
            for variable in typesDict:
                cur_type = typesDict[variable]
                
                if cur_type == "integer":
                    unique_vals = self.getUnique(in_d, variable)
                    jsonDict['meta'][variable] = {'min': int(min(unique_vals)),
                                        'max': int(max(unique_vals)),
                                        'options':'continuous'}
                elif cur_type == "real":
                    unique_vals = self.getUnique(in_d, variable)
                    jsonDict['meta'][variable] = {'min': float(min(unique_vals)),
                                        'max': float(max(unique_vals)),
                                        'options':'continuous'}
                else:
                    jsonDict['meta'][variable] = {
                                'numOptions': self.countUnique(in_d, variable),
                                'options': None}
                    if self.countUnique(in_d, variable) < self.JSON_LIMIT:
                        unique_vals = self.getUnique(in_d, variable)
                        jsonDict['meta'][variable]['options'] = sorted(unique_vals)
                    else:
                        self.putOne(in_d, out_d, variable)
            
            jsonDict['meta']['sampleID'] = {'numOptions': self.countSampleIDs(out_d),
                                        'options': None}
            if self.countSampleIDs(out_d) < self.JSON_LIMIT:
                sampleIDs = self.getSampleIDs(out_d)
                jsonDict['meta']['sampleID']['options'] = sampleIDs
            
        else:
            jsonDict['meta'] = None
            self.putAll(in_d, out_d, typesDict)

        if not os.path.exists(out_d):
            os.mkdir(out_d)

        out_f = open('/'.join([out_d, 'metadata.json']), 'w')
        json.dump(jsonDict, out_f, 
                    sort_keys=True, indent=4, 
                    separators=(',', ': '))
        out_f.close()
        print("json_maker: " + str(time.time() - self.seconds) +
                " seconds")



if __name__ == "__main__":
   print('\033[91m[WARNING]\033[0m ' +
         'This program is not to be used outside of type_converter.py')