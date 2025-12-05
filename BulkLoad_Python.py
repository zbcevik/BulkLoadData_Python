import os
import json
import time
from pathlib import Path
import click
from pyDataverse.api import NativeApi

# Configuration
API_TOKEN = os.getenv("DATAVERSE_API_TOKEN")  # Read from environment variable
BASE_URL = "https://demo.borealisdata.ca"  # Your Dataverse URL
DATAVERSE_ALIAS = "root"  # The dataverse where datasets will be uploaded
DATASETS_FOLDER = "/Destop/Datasets"  # Main folder containing all dataset folders

# Initialize API
api = NativeApi(BASE_URL, API_TOKEN)

def load_json_metadata(json_path):
    """Load and parse JSON metadata file."""
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        print(f"✓ Loaded metadata from {json_path}")
        return metadata
    except Exception as e:
        print(f"✗ Error loading {json_path}: {e}")
        return None

def create_dataset(metadata, dataverse_alias):
    """Create a new dataset in Dataverse."""
    try:
        # Extract just the dataset version metadata for creation
        if 'datasetVersion' in metadata:
            dataset_json = {
                "datasetVersion": metadata['datasetVersion']
            }
        else:
            dataset_json = metadata

        # Dataverse will reject (HTTP 400) attempts to add files in the
        # dataset JSON unless the caller is a superuser. Remove any file
        # entries from the payload so we create the dataset metadata only,
        # then upload files separately via the upload API.
        dv_payload = dataset_json.get('datasetVersion') if isinstance(dataset_json, dict) else None
        if isinstance(dv_payload, dict):
            if 'files' in dv_payload:
                dv_payload.pop('files', None)
                print("ℹ Removed 'files' from dataset payload to avoid superuser-only API error")
            if 'dataFiles' in dv_payload:
                dv_payload.pop('dataFiles', None)
                print("ℹ Removed 'dataFiles' from dataset payload to avoid superuser-only API error")

        response = api.create_dataset(dataverse_alias, dataset_json)

        if response.status_code == 201:
            data = response.json()
            dataset_id = data['data']['id']
            persistent_id = data['data']['persistentId']
            print(f"✓ Created dataset: {persistent_id}")
            return persistent_id
        else:
            print(f"✗ Failed to create dataset: {response.status_code}")
            print(f"  Response: {response.json()}")
            return None
    except Exception as e:
        print(f"✗ Error creating dataset: {e}")
        return None

def delete_all_files_in_dataset(persistent_id):
    """Delete all files currently attached to a dataset (by persistent id)."""
    try:
        resp = api.get_datafiles_metadata(persistent_id, version=":latest")
        if resp.status_code != 200:
            print(f"⚠ Could not list files for {persistent_id}: {getattr(resp, 'status_code', '<no status>')}")
            return False

        data = resp.json().get("data", [])
        if not data:
            print(f"ℹ No existing files to delete for {persistent_id}")
            return True

        for item in data:
            file_id = None
            # Typical response shape: item['dataFile']['id']
            if isinstance(item, dict):
                df = item.get("dataFile")
                if isinstance(df, dict):
                    file_id = df.get("id")

            if not file_id:
                print(f"⚠ Could not determine file id for item: {item}")
                continue

            url = f"{api.base_url_api_native}/files/{file_id}"
            del_resp = api.delete_request(url)
            status = getattr(del_resp, 'status_code', '<no status>')
            if status in [200, 204]:
                print(f"✓ Deleted file id {file_id} from {persistent_id}")
            else:
                try:
                    dbg = del_resp.json()
                except Exception:
                    dbg = getattr(del_resp, 'text', '<no body>')
                print(f"✗ Failed to delete file id {file_id}: status={status} body={dbg}")

        return True
    except Exception as e:
        print(f"✗ Error deleting files for {persistent_id}: {e}")
        return False

def upload_zip_file(persistent_id, zip_path):
    """Upload a ZIP file to the dataset."""
    try:
        # Ensure we pass a filesystem path string to pyDataverse, which
        # will open the file itself. If a Path object was provided, cast
        # to str so pyDataverse can open it.
        filepath = str(zip_path)
        filename = os.path.basename(filepath)

        response = api.upload_datafile(persistent_id, filepath)

        # Diagnostic output: always print status and response body/text
        try:
            body = response.json()
        except Exception:
            body = getattr(response, "text", "<no body>")
        print(f"[upload diagnostic] status={getattr(response, 'status_code', '<no status>')} body={body}")

        if response.status_code in [200, 201]:
            print(f"✓ Uploaded file: {filename}")
            return True
        else:
            print(f"✗ Failed to upload {filename}: {response.status_code}")
            try:
                print(f"  Response: {response.json()}")
            except Exception:
                print(f"  Response text: {getattr(response, 'text', '<no text>')}")
            return False
    except Exception as e:
        print(f"✗ Error uploading {zip_path}: {e}")
        return False

def process_dataset_folder(dataset_folder_path):
    """Process a single dataset folder (JSON + ZIP)."""
    print(f"\n{'='*60}")
    print(f"Processing: {dataset_folder_path.name}")
    print(f"{'='*60}")

    # Skip folder if it already contains a per-file uploaded marker
    markers = list(dataset_folder_path.glob('.uploaded_*'))
    if markers:
        # If any marker exists, assume the dataset was already processed
        print(f"ℹ Skipping {dataset_folder_path.name}: marker found ({markers[0].name})")
        return True

    # Find JSON and ZIP files
    json_files = list(dataset_folder_path.glob("*.json"))
    zip_files = list(dataset_folder_path.glob("*.zip"))

    if not json_files:
        print(f"✗ No JSON file found in {dataset_folder_path.name}")
        return False

    if not zip_files:
        print(f"⚠ No ZIP file found in {dataset_folder_path.name}")
        # Continue anyway - some datasets might not have files

    # Use the first JSON file found
    json_file = json_files[0]

    # Load metadata
    metadata = load_json_metadata(json_file)
    if not metadata:
        return False

    # Create dataset
    persistent_id = create_dataset(metadata, DATAVERSE_ALIAS)
    if not persistent_id:
        return False

    # Always delete existing files attached to the dataset before uploading
    deleted = delete_all_files_in_dataset(persistent_id)
    if not deleted:
        print(f"⚠ Could not delete existing files for {persistent_id}; continuing to upload")

    # Upload ZIP file if exists
    if zip_files:
        zip_file = zip_files[0]
        upload_success = upload_zip_file(persistent_id, zip_file)
        if not upload_success:
            print(f"⚠ Dataset created but file upload failed")

    # Wait a bit to avoid overwhelming the server
    time.sleep(1)

    print(f"✓ Completed: {dataset_folder_path.name}")
    return True


@click.command()
@click.option('--api-token', envvar='DATAVERSE_API_TOKEN', help='Dataverse API token')
@click.option('--base-url', default='https://demo.borealisdata.ca', help='Dataverse base URL')
@click.option('--dataverse-alias', default='root', help='Dataverse alias for dataset upload')
@click.option('--datasets-folder', default='/Destop/Datasets', help='Path to datasets folder')
def main(api_token, base_url, dataverse_alias, datasets_folder):
    """Bulk load datasets to Dataverse."""
    if not api_token:
        click.echo("Error: API token not provided. Set DATAVERSE_API_TOKEN environment variable or use --api-token option.")
        raise click.Exit(1)
    
    click.echo(f"Starting bulk load...")
    click.echo(f"Base URL: {base_url}")
    click.echo(f"Dataverse Alias: {dataverse_alias}")
    click.echo(f"Datasets Folder: {datasets_folder}")
    
    # Initialize globals from CLI options so other functions use the correct values
    global api, API_TOKEN, BASE_URL, DATAVERSE_ALIAS, DATASETS_FOLDER
    API_TOKEN = api_token
    BASE_URL = base_url
    DATAVERSE_ALIAS = dataverse_alias
    DATASETS_FOLDER = datasets_folder
    api = NativeApi(BASE_URL, API_TOKEN)

    datasets_path = Path(DATASETS_FOLDER).expanduser()
    if not datasets_path.exists():
        click.echo(f"Error: datasets folder '{datasets_path}' does not exist.")
        raise click.Exit(1)

    # Iterate over subdirectories and process each dataset folder
    for entry in sorted(datasets_path.iterdir()):
        if entry.is_dir():
            process_dataset_folder(entry)

if __name__ == '__main__':
    main()

#python BulkLoad_Python.py --base-url https://demo.borealisdata.ca --dataverse-alias zeynepcevik --datasets-folder /workspaces/BulkLoadData_Python/Datasets