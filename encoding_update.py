import gzip
import os
import pdb

initial_path = 'data/feeds'
final_path = 'data/encoded_feeds'

files = os.listdir(initial_path)

for file in files:
    if file.find('.gz') > 0:
        # pdb.set_trace()
        print 'Start - encoding: ' + file
        fp = gzip.open('/'.join([initial_path, file]))
        contents = fp.read() # contents now has the uncompressed bytes of foo.gz
        fp.close()
        u_str = contents.decode('ISO-8859-1').encode('utf-8') # u_str is now a unicode string
        if not os.path.isdir(final_path):
            os.mkdir(final_path)
        
        outfilename = '/'.join([final_path,'encoded-'+file])
        output = gzip.open(outfilename, 'wb')
        try:
            output.write(u_str)
            
        finally:
            output.close()
        print 'End - encoding: ' + file