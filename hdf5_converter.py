import sys
import pandas as pd
import h5py
import gzip
import codecs

class H5Converter:
    def numberRows(self, in_p):
        num_rows = -1
        with gzip.open(in_p,'r') as in_f:
            for i, row in enumerate(in_f):
                num_rows = i
        return num_rows


    # Arguments:
    #	[1] InputPath
    #	[2] Transcript Split (where metadata ends)
    #	[3] New HDF5 file

    def convert(self, in_p, transcriptSplit, out_p):

        num_rows = self.numberRows(in_p)


        num_cols = 0
        with gzip.open(in_p,'r') as in_f:
            line = codecs.decode(in_f.readline()).split('\t')
            #print(','.join(line))
            num_cols = len(line)

        #Initialize hdf5 file
        hdf = h5py.File(out_p,'a')

        #Create group under original file name
        #grp = hdf.create_group(in_p.split('/')[-1])

        #Free up space for dataset
        data = hdf.create_dataset('data',(num_rows,num_cols - transcriptSplit))

        counter = 0
        df = pd.read_csv(in_p,usecols = list(range(transcriptSplit,num_cols)), sep='\t', chunksize=100)

        #Put chunks into dataset
        while counter < num_rows:
            chunk = df.get_chunk()
            data[counter:counter + 100, :] = \
                        chunk.values
            counter += 100


if __name__ == "__main__":
    print("\033[91m[WARNING] \033[0m" + 
            "This program is to be used exclusively in type_converter.py")

