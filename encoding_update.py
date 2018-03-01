import gzip
import os
import pdb

initial_path = 'data/feeds'
final_path = 'data/encoded_feeds'

files = os.listdir(initial_path)

blocksize = 1 << 16     #64kB

for file in files:
    if file.find('.gz') > 0:
        try:
            outfilename = '/'.join([final_path,'encoded-'+file])
            if not os.path.isdir(final_path):
                os.mkdir(final_path)
            
            # pdb.set_trace()
            print 'Start - encoding: ' + file
            fp = gzip.open('/'.join([initial_path, file]))
            output = gzip.open(outfilename, 'wb')

            while True:
                    contents = fp.read(blocksize)
                    if contents == '':
                        break
                    output.write(contents.decode('ISO-8859-1').encode('utf-8'))
            fp.close()
            output.close()
            # contents = fp.read() # contents now has the uncompressed bytes of foo.gz
            
            # u_str = contents.decode('ISO-8859-1').encode('utf-8') # u_str is now a unicode string
            print 'End - encoding: ' + str(file)
        except:
            print 'Error on file: ' + str(file)