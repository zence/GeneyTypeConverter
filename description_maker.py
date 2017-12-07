import json
import sqlite3
import datetime
import math
import yaml

class DescriptionMaker:

    def __init__(self, in_d, out_d):
        self.in_d = in_d
        self.out_d = out_d

    def getFeatureCount(self):
        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT count(*)
                            FROM featureTable''')

        return curs.fetchone()[0]

    def getSampleCount(self):
        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT count(*)
                            FROM sampleTable''')

        return curs.fetchone()[0]

    def getVarCount(self):
        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT count(*)
                            FROM variableTable''')

        return curs.fetchone()[0]

    def make_description(self):
        epoch = datetime.datetime.utcfromtimestamp(0)

        curDate = int(math.ceil(
            (datetime.datetime.today() - epoch).total_seconds()))
        

        cur_id = self.in_d.split('/')[-1]
        descStream = open('/'.join([self.in_d, "description.md"]), 'r')
        description = descStream.read()
        descStream.close()


        stream = open('/'.join([self.in_d, "config.yaml"]), 'r')
        yamlDict = yaml.load(stream)
        stream.close()

        jsonDescript = {
            'numMetaTypes': self.getVarCount(), 
            'numSamples': self.getSampleCount(),
            'numFeatures': self.getFeatureCount(), 
            'description': description,
            'id': cur_id, 
            'uploadDate': curDate,}

        jsonDescript.update(yamlDict)

        with open(self.out_d + "/description.json",'w') as out_f:
            out_f.write(
                json.dumps(
                    jsonDescript, sort_keys=True, indent=4, 
                    separators=(',', ': ')))

if __name__ == "__main__":
    print("\033[91m[WARNING] \033[0m" + 
        "This program is to be used exclusively within sql_converter.py!")