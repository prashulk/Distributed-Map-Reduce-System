
#start the VM
gcloud compute instances start praskumainstance2
gcloud compute instances list
gcloud compute scp /Users/prashulkumar/Documents/SEM-3/ECC/Assignment-4/prashul-kumar-fall2023-23ecf366ca39.json praskumainstance2

#Provide authorization to modify the praskumabucket
export GOOGLE_APPLICATION_CREDENTIALS=~/prashul-kumar-fall2023-23ecf366ca39.json


#test case 1:
curl -X POST https://us-central1-prashul-kumar-fall2023.cloudfunctions.net/master

