import optparse
import os
import BMK.Tools as BMKT
parser = optparse.OptionParser()
parser.add_option('-P', '--PoolMix', action="store", dest="PoolName", help="Run name of poolmix", default=False)
options, args = parser.parse_args()

## Move good Fastq to General Folder
BMKT.FastqMovement(options.PoolName, 'General')

## Move Fastq from General Good Folder to FTP client

BMKT.FastqMovement(options.PoolName, 'FTP')


