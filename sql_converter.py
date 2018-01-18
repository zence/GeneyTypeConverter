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
        self.CHUNK_SIZE = 10000

    def addSample(self, sampleName, conn):
        sampleID = 0
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

    def addFeatures(self, conn):
        features = []
        with gzip.open('/'.join([self.in_d, 'data.tsv.gz']), 'r') as in_f:
            features = codecs.decode(in_f.readline()).rstrip().split('\t')[1:]
        curs = conn.cursor()

        with conn:
            for ix, feature in enumerate(features):
                curs.execute('''INSERT INTO featureTable
                                VALUES (''' + str(ix) +
                                ',"' + feature + '")')

    def getType(self, variableName, conn):
        conn = sqlite3.connect('/'.join([self.out_d, 
                                    'metadata.sqlite']))
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT variableType
                            FROM variableTable
                            WHERE variableName = "''' + variableName + '"')

        return curs.fetchone()[0]

    def createTables(self, conn):
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

    def getSampleID(self, sampleName, conn):
        #NOTE: Can only be called AFTER sampleID is inserted
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT sampleID
                            FROM sampleTable
                            WHERE sampleName = "''' + sampleName + '"')
            
        return curs.fetchone()[0]

    def getVarID(self, varName, conn):
        #NOTE: Should work as long as this is only called after
        #   variableTable is made in type_converter
        curs = conn.cursor()

        with conn:
            curs.execute('''SELECT variableID
                            FROM variableTable
                            WHERE variableName = "''' + varName + '"')

        return curs.fetchone()[0]

    def addInt(self, ints, conn):
        curs = conn.cursor()
        curs.executemany("INSERT INTO integerTable VALUES (?,?,?)", ints)

    def addReal(self, reals, conn):
        curs = conn.cursor()
        curs.executemany("INSERT INTO realTable VALUES (?,?,?)", reals)

    def addText(self, texts, conn):
        curs = conn.cursor()

        curs.executemany("INSERT INTO textTable VALUES (?,?,?)", texts)

    def makeDescription(self):
        descriptionMaker = DescriptionMaker(self.in_d, self.out_d)
        descriptionMaker.make_description()

    def convert(self):

        conn = sqlite3.connect('/'.join([self.out_d,
                                            'metadata.sqlite']))
        self.createTables(conn)

        in_file = gzip.open('/'.join([self.in_d, 'metadata.tsv.gz']), 'r')
        header = in_file.readline()
        cur_type = ''
        self.addFeatures(conn)
        

        ints = []
        reals = []
        texts = []

        for ix, line in enumerate(in_file):
            line = codecs.decode(line).rstrip().split('\t')

            
            self.addSample(line[0], conn)
            sampleID = self.getSampleID(line[0], conn)
            variable = line[1]
            value = line[2]

            cur_type = self.getType(variable, conn)

            variable = self.getVarID(variable, conn)

            if cur_type == "integer":
                ints.append((sampleID, variable, value))
            elif cur_type == "real":
                reals.append((sampleID, variable, value))
            else:
                texts.append((sampleID, variable, value))

            if ix % self.CHUNK_SIZE == 0 and ix > 0:
                self.addInt(ints, conn)
                self.addReal(reals, conn)
                self.addText(texts, conn)
                conn.commit()
                ints = []
                reals = []
                texts = []

            final_ix = ix
        if len(ints) > 0:
            self.addInt(ints, conn)
            self.addReal(reals, conn)
            self.addText(texts, conn)
        conn.commit()
        
        print("sql_converter: " + str(time.time() - self.seconds) +
                " seconds")
        self.makeDescription()

if __name__ == "__main__":
    print("\033[91m[WARNING] \033[0m" + 
            "This program is to be used exclusively in type_converter.py")