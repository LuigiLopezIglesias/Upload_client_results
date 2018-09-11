import optparse
import BMK.Queries as BMKQ
import BMK.Tools   as BMKTo

parser = optparse.OptionParser()

parser.add_option('-P', '--PoolMix', action="store", dest="Run", help="Run name of poolmix", default=False)
options, args = parser.parse_args()

## STEP1: Upload good fastq and not BMK
BMKTo.WsFastqMovement(options.Run)

## STEP2: Upload finished BMK sample Fastq to repository bucket and Client FTP, and upload excel file to FTP
BMKTo.ClientsAbundances(options.Run)

## STEP3: Metadata generation to client and sample type
BMKTo.ClientsMetadataCreation(options.Run)

## STEP4:  Give information of samples to comunicate to owners
BMKTo.ClientsToOwner(Options.Run)
