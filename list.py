import os

files = {}
for f in os.listdir('data'):
    files[f] = open('data/' + f).read()

for k, v in files.items():
    print "\"%s\",\"%s\"" % (k, v.replace('\n', ' '))
