import optparse
import os
import BMK.Queries as BMKQ
import BMK.Tools   as BMKTo
import BMK.Utiles  as BMKU
import git

parser = optparse.OptionParser()

parser.add_option('-D', '--Date', action="store", dest="Date", help="Date of poolmix", default=os.environ['RunDate'])
options, args = parser.parse_args()

# STEP1: Upload good fastq and not BMK
BMKTo.WsFastqMovement(options.Date+"POOLMIX")

# STEP2: Upload finished BMK sample Fastq to repository bucket and Client FTP, and upload excel file to FTP
BMKTo.ClientsAbundances(options.Date+"POOLMIX")

# STEP3: Metadata generation to client and sample type
BMKTo.ClientsMetadataCreation(options.Date+"POOLMIX")

# STEP4:  Give information of samples to comunicate to owners
BMKTo.ClientsToOwner(options.Date+"POOLMIX")
