import sqlite3
import gzip
import codecs
import argparse
import os
import fastnumbers
import time
import shutil
from json_maker import JsonMaker
from sql_converter import Converter
from hdf5_converter import H5Converter
from description_maker import DescriptionMaker

#   1) Read file into sqlite database
#   2) Put all distinct variables in sets
#          [Text, Real, Integer]
#   3) Divide them according to hierarchy
#   Text -> Real -> Integer
#   If it has text, put in text
#   Else if it has floats, put in real
#   Else Integer

# TODO: Create json file for unique values

class UniqueVarChecker:

    def checkType(self, typesDict, variable, value):
        """
            Check variable's type
        """
        if variable in typesDict:
            if typesDict[variable] == "text":
                return typesDict
            elif typesDict[variable] == "real":
                if not fastnumbers.isreal(value):
                    typesDict[variable] = "text"
                return typesDict
            elif typesDict[variable] == "integer":
                if not fastnumbers.isint(value):
                    if fastnumbers.isfloat(value):
                        typesDict[variable] = "real"
                    else:
                        typesDict[variable] = "text"
                return typesDict
        elif fastnumbers.isint(value):
            typesDict[variable] = "integer"
        elif fastnumbers.isfloat(value):
            typesDict[variable] = "real"
        else:
            typesDict[variable] = "text"
        return typesDict
                


    def prepTable(self, in_d):
        """
            Creates temp.sqlite
        """
        if os.path.exists('/'.join([in_d, 'temp.sqlite'])):
            os.remove('/'.join([in_d, 'temp.sqlite']))
        conn = sqlite3.connect('/'.join([in_d, 'temp.sqlite']))
        curs = conn.cursor()
        with conn:
            curs.execute('''CREATE TABLE tempTable
                            (variable TEXT NOT NULL,
                            value TEXT NOT NULL)''')


    def makeTable(self, in_d):
        """
            Fills temp.sqlite and returns @typesDict
        """
        conn = sqlite3.connect('/'.join([in_d, 'temp.sqlite']))
        curs = conn.cursor()
        typesDict = {}
        with conn:
            with gzip.open('/'.join([in_d,'metadata.tsv.gz']), 'r') as in_f:
                in_f.readline()
                for line in in_f:
                    line = codecs.decode(line).rstrip().split('\t')
                    variable = line[1]
                    value = line[2]
                    typesDict = self.checkType(typesDict, variable, value)
                    curs.execute('''INSERT INTO tempTable
                                    VALUES ("''' + variable + '''"
                                    , "''' + value + '")')
        return typesDict




    def makeFinal(self, in_d, out_d, typesDict):
        """
            Final method to run/Runs other python classes
        """
        
        
        conn = sqlite3.connect('/'.join([out_d, 'metadata.sqlite']))
        curs = conn.cursor()

        
        
        with conn:
            curs.execute('''CREATE TABLE variableTable
                            (variableID INTEGER PRIMARY KEY NOT NULL,
                            variableName TEXT NOT NULL,
                            variableType TEXT NOT NULL,
                            numOptions INTEGER,
                            options TEXT)''')
        
        with conn:
            for var_ix, variable in enumerate(typesDict):
                cur_type = typesDict[variable]
                curs.execute('''INSERT INTO variableTable
                                VALUES (''' + str(var_ix) + ''',
                                "''' + variable + '''"
                                , "''' + cur_type + '''",
                                NULL, NULL)''')
                
        
        print("type_converter: " + str(time.time() - self.seconds) +
                " seconds")
        converter = Converter(in_d, out_d)
        converter.convert()
        jsonMaker = JsonMaker()
        jsonMaker.createJson(in_d, out_d, typesDict)
        h5Converter = H5Converter()
        h5Converter.convert('/'.join([in_d, 'data.tsv.gz']), 1,
                    '/'.join([out_d, 'data.h5']))
        #print("\n\033[5;95m^_^ ありがとうございました ^_^\033[0m \n\nThank you for using!")

        #NOTE: test_converter.py will not function properly if this line is
        #           not commented out
        #os.remove('/'.join([in_d, 'temp.sqlite']))
        


    def check(self, in_d, out_d, force):
        """
            To be called from Main
            NOTE: May be unnecessary at this point
            NOTE: v1.0.1 '/' is now allowed at the end of directory paths
        """
        in_d = in_d.rstrip('/')
        out_d = out_d.rstrip('/')
        if not os.path.exists(in_d):
            print("\033[91m[ERROR]\033[0m Input path not found :(")
            return
        if os.path.exists(out_d):
            if not force:
                print("\033[93m[WARNING]\033[0m Output directory ["
                        + out_d + "] already exists, delete? [y/n] (default: y)")
                try:
                    reply = input()
                except EOFError:
                    reply = 'y'
                if reply.lower() == 'y' or reply.lower() == 'yes':
                    shutil.rmtree(out_d)
                    os.mkdir(out_d)
                else:
                    print('\033[94mExiting\033[0m')
                    return
            else:
                shutil.rmtree(out_d)
                os.mkdir(out_d)
        else:
            os.mkdir(out_d)
        self.seconds = time.time()

        #NOTE: I no longer use these, but will keep this for reference
        #   Basically, this method is now only four lines long

        #   For the real set:
        #       NOTE: used to use printf("%f", value) = value, however, if value == 1.7,
        #           this would compare 1.70000 = 1.7
        #         Since we are already taking out any values that have any letters, we
        #           can just GLOB for DIGIT.DIGIT surrounded by zero to infinite values 
        #realQuery = '''SELECT DISTINCT variable FROM tempTable WHERE value GLOB "*[0-9].[0-9]*"'''

        #   For the integer set:
        #intQuery = '''SELECT DISTINCT variable FROM tempTable WHERE printf("%d", value) = value'''

        #   For the text set:
        #   WARNING: "*[A-Za-z]*" searches to make sure there is at least one letter in
        #       the entire value. MAKE SURE THIS IS WHAT YOU WANT
        #textQuery = '''SELECT DISTINCT variable FROM tempTable WHERE value GLOB "*[^0-9]*"'''

        self.prepTable(in_d)
        typesDict = self.makeTable(in_d)

        self.makeFinal(in_d, out_d, typesDict)

    def justDescription(self, in_d, out_d):
        """
            Called if '-s' is given
        """
        in_d = in_d.rstrip('/')
        out_d = out_d.rstrip('/')
        if not os.path.exists(in_d):
            print("\033[91m[ERROR]\033[0m Input path not found :(")
            return
        descriptionMaker = DescriptionMaker(in_d, out_d)
        descriptionMaker.make_description()

class Main:

    def __init__(self):
        args = self.parse_args()
        self.checker = UniqueVarChecker()
        if not args.simple:
            self.checker.check(args.input_directory, args.output_directory, args.force)
        else:
            self.checker.justDescription(args.input_directory, args.output_directory)

    def parse_args(self):
        parsInputHelp = "path to directory where data exists and output "
        parsInputHelp += "will be stored"
        parsOutputHelp = "path to directory where data will be stored "
        parsOutputHelp += "with sqlite and hdf5 files"
        parser = argparse.ArgumentParser(description="Check all unique variable types")
        parser.add_argument("input_directory", type=str,
                            help=parsInputHelp)
        parser.add_argument("output_directory", type=str,
                            help=parsOutputHelp)
        parser.add_argument("-f", "--force", action="store_true",
                            help="run program without warnings")
        parser.add_argument("-s", "--simple", action="store_true",
                            help="run simplified version of program")
        args = parser.parse_args()
        
        return args

if __name__ == "__main__":
    Main()