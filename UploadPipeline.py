import optparse
import os
import BMK.Queries as BMKQ
import BMK.Tools   as BMKTo
import BMK.Utiles  as BMKU
import git

parser = optparse.OptionParser()

parser.add_option('-D', '--Date', action="store", dest="Date", help="Date of poolmix", default=os.environ['RunDate'])
options, args = parser.parse_args()

## STEP0: Git
#BMKU.create_dir(os.getcwd()+'Run4Jenkins/'+options.Date)
#BMKU.create_dir('/Results'+'/'+options.Date)
#repo = git.Repo.clone_from('git@github.com:BiomeMakers/Run4Jenkins.git', '/Run4Jenkins/')
#for marker in ['16s', 'its']:
# # Change branch to take information about hash
# print("Changing branch to \x1b[1;36;10m"+options.Date+"_"+marker+"\x1b[0m")
# repo.git.checkout(options.Date+'_'+marker.upper())
# # Download biom files
# print("Downloading mapped biom file to \x1b[1;36;10m"+os.getcwd()+'Results'+'/'+options.Date+"\x1b[0m")
# BMKU.fileDownloader(options.Date, marker, '/Results', '/Run4Jenkins/')


# STEP1: Upload good fastq and not BMK
BMKTo.WsFastqMovement(options.Date+"POOLMIX")

# STEP2: Upload finished BMK sample Fastq to repository bucket and Client FTP, and upload excel file to FTP
BMKTo.ClientsAbundances(options.Date+"POOLMIX")
#
### STEP3: Metadata generation to client and sample type
#BMKTo.ClientsMetadataCreation(options.Date)
#
### STEP4:  Give information of samples to comunicate to owners
#BMKTo.ClientsToOwner(options.Date)
