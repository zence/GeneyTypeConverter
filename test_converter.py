import unittest
import os
import pandas
import h5py
import pandas
import json
from type_converter import *

TEST_IN_DIR = "Test_Data"
TEST_OUT_DIR = "Test_Case"

#NOTE: This test can only be run if "Test_Data/" has the appropriate files
#       If you wish to be able to run this test, please copy the following into
#   appropriate files:

#FILE: metadata.tsv.gz <---(Files must be gzipped):

#Sample  Variable        Value
#Kevin   Alcohol_Consumption     Yes
#Kevin   Smoking_History Of_Course
#Kevin   Tumors  3
#Kevin   Paranoia        2.5
#Kevin   Example_X       1
#Jamie   Alcohol_Consumption     No
#Jamie   Smoking_History Nope
#Jamie   Tumors  0
#Jamie   Paranoia        7.34
#Jamie   Example_X       0
#Prometheus      Alcohol_Consumption     Yes
#Prometheus      Smoking_History Nope
#Prometheus      Tumors  1
#Prometheus      Paranoia        0
#Prometheus      Example_X       X

#FILE: data.tsv.gz <---(Again, create the data.tsv file, and THEN gzip):

#Sample  Joy     Sadness Sarcasm
#Kevin   1.234567        -0.232  0.0
#Jamie   1.543456        0.232   1.23
#Prometheus      0.0     0.1     -2.345

#FILE: config.yaml <---():

#title: Test Data
#featureDescription: gene
#featureDescriptionPlural: genes

#FILE: description.md <---(Just needs to be in markdown format CONTENT IS IRRELEVANT)



class TestSetup(unittest.TestCase):
    def test_BadCsvFails(self):
        dummy_file_path = "test/bad.csv"
        with self.assertRaises(Exception):
            pandas.read_csv(dummy_file_path)
        self.assertNotAlmostEqual(1.2345, 1.1234)

class TestConversion(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        try:
            os.rmdir(TEST_OUT_DIR)
        except:
            pass
        conv = UniqueVarChecker()
        conv.check(TEST_IN_DIR, TEST_OUT_DIR)
        cls.hdf = h5py.File('/'.join([TEST_OUT_DIR, "data.h5"]), 'r')
        cls.conn = sqlite3.connect('/'.join([TEST_OUT_DIR, "metadata.sqlite"]))
        stream = open('/'.join([TEST_OUT_DIR, "description.json"]), 'r')
        cls.description = json.loads(stream.read())
        stream.close()
        stream = open('/'.join([TEST_OUT_DIR, 'metadata.json']), 'r')
        cls.metaJson = json.loads(stream.read())
        stream.close()

    @classmethod
    def tearDownClass(cls):
        cls.hdf = None
        cls.conn = None
        cls.description = None

    def test_DirectoryExists(self):
        self.assertTrue(os.path.isdir(TEST_OUT_DIR))
        self.assertIsNotNone(self.__class__.hdf)
    
    def test_Description(self):
        description = self.__class__.description
        self.assertEqual(description['featureDescription'], 'gene')
        self.assertEqual(description['featureDescriptionPlural'], 'genes')
        self.assertEqual(description['id'], 'Test_Data')
        self.assertEqual(description['numFeatures'], 3)
        self.assertEqual(description['numMetaTypes'], 5)
        self.assertEqual(description['numSamples'], 3)
        self.assertEqual(description['title'], 'Test Data')

        markDown = ''
        with open('/'.join([TEST_IN_DIR, 'description.md']), 'r') as in_f:
            markDown = in_f.read()
        self.assertEqual(description['description'], markDown)

    def test_SQLite(self):
        curs = self.__class__.conn.cursor()
        samples = ["Prometheus", "Kevin", "Jamie"]
        curs.execute('''SELECT sampleName
                        FROM sampleTable''')
        cur_list = curs.fetchall()
        for sample in cur_list:
            self.assertTrue(sample[0] in samples)


        variables = ["Alcohol_Consumption", "Smoking_History",
                     "Tumors", "Paranoia", "Example_X"]
        curs.execute('''SELECT variableName
                        FROM variableTable''')
        cur_list = curs.fetchall()
        for variable in cur_list:
            self.assertTrue(variable[0] in variables)

        curs.execute('''SELECT sampleTable.sampleName
                        FROM textTable 
                        JOIN sampleTable
                            ON sampleTable.sampleID = textTable.sampleID
                        JOIN variableTable
                            ON variableTable.variableID = textTable.variableID
                        WHERE variableTable.variableName = "Alcohol_Consumption"
                        AND textTable.value = "Yes"''')

        self.assertEqual(curs.fetchone()[0], "Kevin")

        curs.execute('''SELECT sampleTable.sampleName
                        FROM realTable 
                        JOIN sampleTable
                            ON sampleTable.sampleID = realTable.sampleID
                        JOIN variableTable
                            ON variableTable.variableID = realTable.variableID
                        WHERE variableTable.variableName = "Paranoia"
                        AND realTable.value = 7.34''')

        self.assertEqual(curs.fetchone()[0], "Jamie")

        curs.execute('''SELECT sampleTable.sampleName
                        FROM textTable 
                        JOIN sampleTable
                            ON sampleTable.sampleID = textTable.sampleID
                        JOIN variableTable
                            ON variableTable.variableID = textTable.variableID
                        WHERE variableTable.variableName = "Paranoia"
                        AND textTable.value = 7.34''')

        self.assertEqual(len(curs.fetchall()), 0)

        curs.execute('''SELECT sampleTable.sampleName
                        FROM integerTable 
                        JOIN sampleTable
                            ON sampleTable.sampleID = integerTable.sampleID
                        JOIN variableTable
                            ON variableTable.variableID = integerTable.variableID
                        WHERE variableTable.variableName = "Tumors"
                        AND integerTable.value = 1''')

        self.assertEqual(curs.fetchone()[0], "Prometheus")
        
        
        

    def test_hdf5(self):
        df = pandas.read_csv('/'.join([TEST_IN_DIR, "data.tsv.gz"]), 
                            sep='\t', usecols=range(1,4), header=None,
                            skiprows=[0])
        data = self.__class__.hdf['data']
        for i in range(0,3):
            for j in range(0,3):
                self.assertAlmostEqual(data[i][j], df.iloc[i,j])


