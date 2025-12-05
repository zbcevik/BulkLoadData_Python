# BulkLoadData_Python
Loading multiple datasets by using API on Dataverse

In each folder, there should be one zip and a metadata json file. The zip file includes all the files/datasets that you want to upload. The metadata json file should be DDI standard json file.

Run these codes on Terminal in workpace 

- Create a virtual environment: 

`python -m venv venv`

- Activate the environment:

`source venv/bin/activate`

- Install pip packages:

`pip install -r requirements.txt`

- Get API token, create new environment variable:

`export DATAVERSE_API_TOKEN="your_token_here"`

- To upload the files, you need 1 json file and 1 zip file in each dataset:

`python BulkLoad_Python.py --base-url "https://demo.borealisdata.ca" --dataverse-alias "yourDataverseAlias" --datasets-folder "/your/folder/path/Datasets"`

-To upload documents through One-drive, create a json file like in remote_list.json file which includes all urls for each dataset (You have to be admin in order to use this functionality)

`python3 remote_loader.py \
  --remote-list "json/file/link/one/drive" \
  --onedrive-token "YOUR_GRAPH_TOKEN" \
  --base-url "https://demo.borealisdata.ca" \
  --dataverse-alias "zeynepcevik" `

- To upload the files through Google Drive:

`python3 remote_loader.py \
  --remote-list "link/datasets/gdrive" \
  --gdrive-token "YOUR GDRIVE TOKEN" \
  --base-url "https://demo.borealisdata.ca" \
  --dataverse-alias "zeynepcevik" `

