import gzip
fp = gzip.open('data/feeds/01-debenhamsdotcomauth_2018-02-06.tsv.gz')
contents = fp.read() # contents now has the uncompressed bytes of foo.gz
fp.close()
u_str = contents.decode('ISO-8859-1').encode('utf-8') # u_str is now a unicode string
u_str


outfilename = 'data/feeds/encoded.tsv.gz'
output = gzip.open(outfilename, 'wb')
try:
    output.write(u_str)
finally:
    output.close()
