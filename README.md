# WELCOME TO THE UNNAMED PART OF GENEY

## FUNCTIONALITY
* The Geney Type Converter
    * Call `python type_converter.py [input directory] [output directory]`
    * Will convert `[input directory]` contents into `[output directory]`
    
* `[output directory]` will end up with the following contents:
    * `metadata.sqlite`: SQLite database containing metadata and indices
    * `metadata.json`: Contains filter options for ease in displaying front end
    * `description.json`: Contains info about current dataset
    * `data.h5`: Contains expression (!) data

## IMPORTANT
* `sql_converter`, `hdf5_converter`, `json_maker`, and `description_maker` are *NOT* to be called alone
* `test_converter` is a unittest, but requires a specified test dataset for proper functionality

## UPDATES
* Dec-8-2017:
    * '/' is now allowed at the end of directory paths

* Jan-11-2018:
    * JsonMaker's putOne() function was not working properly. It now runs as it should, as well as differentiating between text and continuous variables, although I now realize that that is unnecessary, since putOne() should never be called on a variable that is an integer or real.
