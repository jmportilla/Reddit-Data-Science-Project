__author__ = 'marci'

import ijson

count = 0
def parse_time():
    count = 0
    #parser = ijson.parse(open('/Volumes/Seagate Backup Plus Drive 1/RC_2015-01'))
    while count < 20:
        for item in ijson.items('/Volumes/Seagate Backup Plus Drive 1/RC_2015-01',"item"):
            print item
            count += 1

if __name__ == '__main__':
    print 'Running!'
    parse_time()
