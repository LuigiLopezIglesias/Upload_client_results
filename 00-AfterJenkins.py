import optparse
import os

parser = optparse.OptionParser()
parser.add_option('-P', '--PoolMix', action="store", dest="Run", help="Run name of poolmix", default=False)
options, args = parser.parse_args()


## Upload Fastq files to repository of good samples
os.system("/home/yamishakka/Escritorio/github/Luigi/Upload_client_results/01-GoodFastqUp.py -P "+options.Run)

## Upload Fastq files to Biomemakers users (FTP)
os.system("/home/yamishakka/Escritorio/github/Luigi/Upload_client_results/02-Fastq_Clients.py -P "+options.Run)

## Upload abundance files to Biomemakers users (FTP)
os.system("/home/yamishakka/Escritorio/github/Luigi/Upload_client_results/03-Abundances_Clients.py -P "+options.Run)

## Upload metadata file of Biomemakers clients (FTP)
os.system("/home/yamishakka/Escritorio/github/Luigi/Upload_client_results/04-Metadata_info.py -P "+options.Run)

## Email to owners with actualized information


