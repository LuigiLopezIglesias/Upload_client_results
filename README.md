# Upload_client_results
Develope script to upload files to FTP

Para instalar los modulos de python mediante el archivo requirements.txt es necesario poner el codigo:

sudo pip install -r requirements.txt

para preparar la base de datos para usar los modulos se necesita:

sudo apt-get build-dep python-psycopg2

## Scripts explication

## Principal Script

The principal script is an schema of all steps that i do after upload results in ANITA in relashionship with FTP clients (no Wineseq) 

### Step 1

In this step the right and finished samples, Fastq of Wineseq are storage in good repository in S3 (*YEAR1231*) and if the right file is a repetition (*SAMPLE-2*, *SAMPLE-3*, ...) this name will be change good to consistent name (*SAMPLE*)

### Step 2

In this case 3 process will be make at the same time:

1. Upload right and finished fastq of Biomemaker samples to client FTP folder with right name

2. Upload right and finished fastq of Biomemaker samples to S3 good repository

3. Upload Excel file of abundances with results in percentaje in client FTP folder

### Step 3

This step makes a creation of Metadata file by Client and sample type that will be storage in FTP folder of client

### Step 4

In this point a list of finished samples will be uploaded to Database where owners will be comunicated of storaged of files for FTP clients (if a wineseq want to be required will be a manual process)

### Execution of docker

`docker build -t luigi/upload:1.00 --build-arg ssh_prv_key="$(cat ./github_rsa)" --build-arg ssh_pub_key="$(cat ./github_rsa.pub)" .`

`docker run -ti --name Upload*Date* --env-file env.list -e RunDate=*Date* luigi/upload:1.00`

e.g.:`docker run -ti --name Upload20190123 --env-file env.list -e RunDate=20190123 luigi/upload:1.00`
