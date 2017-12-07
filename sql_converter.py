import sqlite3
import gzip
import codecs
import time
from description_maker import DescriptionMaker

class Converter:

    def __init__(self, in_d, out_d):
        self.in_d = in_d
        self.out_d = out_d
        self.seconds = time.time()

    def addSample(self, sampleName):
        sampleID = 0
        conn = sqlite3.connect('/'.join([self.out_d,
                                    'metadata.sqlite']))
        curs = conn.cursor()

        ##TODO: Prep this for first insert

        with conn:
            curs.execute('''SELECT count(*)
                            FROM sampleTable''')
            if curs.fetchone()[0] > 0:
                curs.execute('''SELECT count(sampleID)
                                FROM sampleTable
                                WHERE sampleName = "''' + sampleName + '"')
                if curs.fetchone()[0] == 0:
                    curs.execute('''INSERT INTO sampleTable
                                    VALUES ((SELECT MAX(sampleID) + 1
                                            FROM sampleTable),
                                            "''' + sampleName + '")')
            else:
                curs.execute('''INSERT INTO sampleTable
                                VALUES (0,"''' + sampleName + '")')

        return sampleName

    def addFeatures(self):
        features = []
        with gzip.open('/'.join([self.in_d, 'data.tsv.gz']), 'r') as in_f:
            features = codecs.decode(in_f.readline()).rstrip().split('\t')[1:]
        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            for ix, feature in enumerate(features):
                curs.execute('''INSERT INTO featureTable
                                VALUES (''' + str(ix) +
                                ',"' + feature + '")')

    def getType(self, variableName):
        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT variableType
                            FROM variableTable
                            WHERE variableName = "''' + variableName + '"')

        return curs.fetchone()[0]

    def createTables(self):
        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute('''CREATE TABLE textTable
                        (sampleID INTEGER, 
                         variableID INTEGER, 
                         value TEXT)''')

            curs.execute('''CREATE TABLE realTable
                        (sampleID INTEGER, 
                         variableID INTEGER, 
                         value REAL)''')

            curs.execute('''CREATE TABLE integerTable
                            (sampleID INTEGER,
                             variableID INTEGER,
                             value INTEGER)''')

            curs.execute('''CREATE TABLE sampleTable
                        (sampleID INTEGER PRIMARY KEY, 
                         sampleName TEXT)''')

            curs.execute('''CREATE TABLE featureTable
                        (featureID INTEGER PRIMARY KEY, 
                         featureName TEXT)''')

    def getSampleID(self, sampleName):
        #NOTE: Can only be called AFTER sampleID is inserted
        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT sampleID
                            FROM sampleTable
                            WHERE sampleName = "''' + sampleName + '"')
            
        return curs.fetchone()[0]

    def getVarID(self, varName):
        #NOTE: Should work as long as this is only called after
        #   variableTable is made in type_converter
        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT variableID
                            FROM variableTable
                            WHERE variableName = "''' + varName + '"')

        return curs.fetchone()[0]

    def addInt(self, line):
        sampleID = self.getSampleID(line[0])
        variableID = self.getVarID(line[1])
        value = line[2]

        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute("INSERT INTO integerTable VALUES (" +
                            str(sampleID) + ',' + str(variableID) +
                            ',"' + value + '")')

    def addReal(self, line):
        sampleID = self.getSampleID(line[0])
        variableID = self.getVarID(line[1])
        value = line[2]

        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute("INSERT INTO realTable VALUES (" +
                            str(sampleID) + ',' + str(variableID) +
                            ',"' + value + '")')

    def addText(self, line):
        sampleID = self.getSampleID(line[0])
        variableID = self.getVarID(line[1])
        value = line[2]

        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute("INSERT INTO textTable VALUES (" +
                            str(sampleID) + ',' + str(variableID) +
                            ',"' + value + '")')

    def makeDescription(self):
        descriptionMaker = DescriptionMaker(self.in_d, self.out_d)
        descriptionMaker.make_description()

    def convert(self):

        self.createTables()

        in_file = gzip.open('/'.join([self.in_d, 'metadata.tsv.gz']), 'r')
        header = in_file.readline()
        cur_type = ''
        self.addFeatures()

        for line in in_file:
            line = codecs.decode(line).rstrip().split('\t')

            

            sampleID = self.addSample(line[0])
            variable = line[1]
            value = line[2]

            cur_type = self.getType(variable)

            if cur_type == "integer":
                self.addInt(line)
            elif cur_type == "real":
                self.addReal(line)
            else:
                self.addText(line)
        print("sql_converter: " + str(time.time() - self.seconds) +
                " seconds")
        self.makeDescription()

if __name__ == "__main__":
    print("\033[91m[WARNING] \033[0m" + 
            "This program is to be used exclusively in type_converter.py")