"""
remote_loader.py

Utility to process a remote list of datasets (metadata + file URL) and
invoke the existing BulkLoad_Python functions to create datasets and upload files.

Remote list format (JSON): an array of objects, each object may contain:
- "metadata_url": URL or local path to a dataset JSON metadata file
- "metadata": an inline metadata object (alternative to metadata_url)
- "file_url": URL or local path to the file to upload (zip)

Example remote_list.json:
[
  {
    "metadata_url": "https://example.com/datasets/foo.json",
    "file_url": "https://example.com/files/foo.zip"
  },
  {
    "metadata": {"datasetVersion": {...}},
    "file_url": "/local/path/to/bar.zip"
  }
]

Usage:
  python remote_loader.py --remote-list remote_list.json --base-url https://demo.borealisdata.ca --dataverse-alias root

This script downloads remote files to temporary files on disk (streamed),
calls the functions in `BulkLoad_Python.py` to create datasets and upload
files, and removes temporary files afterwards.
"""

import os
import json
import tempfile
import shutil
from pathlib import Path
import requests
import click

# Import existing module
import BulkLoad_Python as bl
from pyDataverse.api import NativeApi
from urllib.parse import urljoin, urlparse
from html.parser import HTMLParser
import base64
import re


def download_to_temp(url_or_path, suffix=None, headers=None, timeout=60):
    """Download a remote URL (or copy a local path) to a temporary file.

    Returns the path to the temp file (caller is responsible for deleting it).
    """
    # If it's a local path, just copy it to a temp file
    if not isinstance(url_or_path, str):
        raise ValueError("url_or_path must be a string")

    if url_or_path.startswith(("http://", "https://")):
        r = requests.get(url_or_path, stream=True, headers=headers, timeout=timeout)
        r.raise_for_status()
        # Choose suffix from URL if not provided
        if not suffix:
            suffix = Path(url_or_path).suffix or ""
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        try:
            with tf as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        except Exception:
            try:
                os.unlink(tf.name)
            except Exception:
                pass
            raise
        return tf.name
    else:
        # treat as local path: copy to a temp file
        p = Path(url_or_path)
        if not p.exists():
            raise FileNotFoundError(f"Local file not found: {url_or_path}")
        if not suffix:
            suffix = p.suffix
        tf = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
        tf.close()
        shutil.copy2(p, tf.name)
        return tf.name


class LinkParser(HTMLParser):
    def __init__(self, base_url):
        super().__init__()
        self.links = []
        self.base = base_url

    def handle_starttag(self, tag, attrs):
        if tag.lower() == 'a':
            for (k, v) in attrs:
                if k.lower() == 'href' and v:
                    self.links.append(urljoin(self.base, v))


def get_links_from_html(url, timeout=30):
    """Fetch an HTML page and return absolute links found on it."""
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    parser = LinkParser(url)
    parser.feed(resp.text)
    return parser.links


def process_remote_folder_url(root_url, api_token, base_url, dataverse_alias, timeout=60):
    """Process a URL that lists dataset folders or files (HTML directory index)."""
    # Fetch root links
    try:
        links = get_links_from_html(root_url, timeout=timeout)
    except Exception as e:
        raise click.ClickException(f"Could not fetch or parse remote folder URL: {e}")

    # Normalize and deduplicate
    seen = set()
    links = [l for l in links if not (l in seen or seen.add(l))]

    # Prefer directories (links ending with '/') as dataset folders
    dir_links = [l for l in links if l.rstrip().endswith('/')]
    if dir_links:
        folder_links = dir_links
    else:
        # No subfolders; treat root as flat listing
        folder_links = [root_url]

    for folder in folder_links:
        click.echo(f"\n--- Processing remote folder: {folder} ---")
        try:
            sublinks = get_links_from_html(folder, timeout=timeout)
        except Exception as e:
            click.echo(f"⚠ Could not read folder {folder}: {e}")
            continue

        # find metadata and zip files in sublinks
        json_links = [l for l in sublinks if l.lower().endswith('.json')]
        zip_links = [l for l in sublinks if l.lower().endswith('.zip')]

        # If multiple jsons, pick the first; same for zip
        metadata_url = json_links[0] if json_links else None
        file_url = zip_links[0] if zip_links else None

        if not metadata_url and folder == root_url:
            # Try to pair flat jsons and zips by name
            root_jsons = [l for l in links if l.lower().endswith('.json')]
            root_zips = [l for l in links if l.lower().endswith('.zip')]
            for j in root_jsons:
                # attempt to find matching zip by stem
                jstem = Path(urlparse(j).path).stem
                match = None
                for z in root_zips:
                    zstem = Path(urlparse(z).path).stem
                    if zstem == jstem:
                        match = z
                        break
                # process this pair
                try:
                    metadata_temp = download_to_temp(j, suffix='.json', timeout=timeout)
                    with open(metadata_temp, 'r', encoding='utf-8') as mf:
                        metadata = json.load(mf)
                    pid = bl.create_dataset(metadata, dataverse_alias)
                    if not pid:
                        click.echo('✗ create_dataset failed; skipping')
                        continue
                    bl.delete_all_files_in_dataset(pid)
                    if match:
                        file_temp = download_to_temp(match, suffix=Path(match).suffix or '.dat', timeout=timeout)
                        bl.upload_zip_file(pid, file_temp)
                        try:
                            os.remove(file_temp)
                        except Exception:
                            pass
                    try:
                        os.remove(metadata_temp)
                    except Exception:
                        pass
                except Exception as e:
                    click.echo(f"✗ Error processing pair {j}/{match}: {e}")
            continue

        if not metadata_url:
            click.echo(f"⚠ No metadata (.json) found in {folder}; skipping")
            continue

        # Download metadata
        metadata_temp = None
        file_temp = None
        try:
            metadata_temp = download_to_temp(metadata_url, suffix='.json', timeout=timeout)
            with open(metadata_temp, 'r', encoding='utf-8') as mf:
                metadata = json.load(mf)

            pid = bl.create_dataset(metadata, dataverse_alias)
            if not pid:
                click.echo('✗ create_dataset failed; skipping this folder')
                continue

            bl.delete_all_files_in_dataset(pid)

            if file_url:
                file_temp = download_to_temp(file_url, suffix=Path(file_url).suffix or '.dat', timeout=timeout)
                bl.upload_zip_file(pid, file_temp)
            else:
                click.echo('ℹ No .zip file found in folder; created dataset without files')

        except Exception as e:
            click.echo(f"✗ Error processing remote folder {folder}: {e}")
        finally:
            for tmp in (metadata_temp, file_temp):
                if tmp:
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
    

def process_google_drive_folder(folder_url, gdrive_token, base_url, dataverse_alias, timeout=60):
    """List files in a Google Drive folder and process .json/.zip files.

    Requires an OAuth2 Bearer token with Drive API access (or a suitable API key
    in more limited public cases). The function uses the Drive v3 REST API.
    """
    # extract folder id from common share URL patterns
    m = re.search(r"/folders/([a-zA-Z0-9_-]+)", folder_url)
    if not m:
        # try query id parameter
        q = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", folder_url)
        if q:
            folder_id = q.group(1)
        else:
            raise click.ClickException("Could not determine Google Drive folder ID from URL")
    else:
        folder_id = m.group(1)

    headers = {"Authorization": f"Bearer {gdrive_token}"}
    list_url = "https://www.googleapis.com/drive/v3/files"
    params = {
        'q': f"'{folder_id}' in parents and trashed=false",
        'fields': 'files(id,name,mimeType,webViewLink,webContentLink)'
    }
    resp = requests.get(list_url, headers=headers, params=params, timeout=timeout)
    resp.raise_for_status()
    items = resp.json().get('files', [])
    if not items:
        click.echo('No files found in Google Drive folder (or insufficient permissions)')
        return

    for it in items:
        name = it.get('name', '')
        fid = it.get('id')
        click.echo(f"Found: {name}")
        if name.lower().endswith('.json'):
            tf = download_to_temp(f"https://www.googleapis.com/drive/v3/files/{fid}?alt=media", suffix='.json', headers=headers, timeout=timeout)
            try:
                with open(tf, 'r', encoding='utf-8') as mf:
                    metadata = json.load(mf)
                pid = bl.create_dataset(metadata, dataverse_alias)
                if pid:
                    bl.delete_all_files_in_dataset(pid)
                else:
                    click.echo('✗ create_dataset failed for Google Drive metadata')
            finally:
                try:
                    os.remove(tf)
                except Exception:
                    pass
        elif name.lower().endswith('.zip'):
            tf = download_to_temp(f"https://www.googleapis.com/drive/v3/files/{fid}?alt=media", suffix='.zip', headers=headers, timeout=timeout)
            # Attempt to find matching dataset by stem? Here we upload to the last-created dataset.
            # Better: require paired metadata JSON entry in the folder. For now, upload as-is to root alias.
            # Create a minimal metadata placeholder
            metadata = {"datasetVersion": {"metadataBlocks": {}}}
            pid = bl.create_dataset(metadata, dataverse_alias)
            if pid:
                bl.delete_all_files_in_dataset(pid)
                bl.upload_zip_file(pid, tf)
            else:
                click.echo('✗ create_dataset failed for Google Drive zip')
            try:
                os.remove(tf)
            except Exception:
                pass


def process_onedrive_share(share_url, onedrive_token, base_url, dataverse_alias, timeout=60):
    """Use Microsoft Graph to list and download items from a share URL.

    Requires a Bearer token with access to Graph APIs. The share URL is encoded
    and used in the `/shares/{shareId}/driveItem/children` endpoint.
    """
    # encode shared URL to Graph shareId format
    b = base64.urlsafe_b64encode(share_url.encode()).decode().rstrip('=')
    share_id = f'u!{b}'
    children_url = f'https://graph.microsoft.com/v1.0/shares/{share_id}/driveItem/children'
    headers = {"Authorization": f"Bearer {onedrive_token}"}
    resp = requests.get(children_url, headers=headers, timeout=timeout)
    if resp.status_code == 401:
        raise click.ClickException('Unauthorized: invalid OneDrive token')
    resp.raise_for_status()
    items = resp.json().get('value', [])
    if not items:
        click.echo('No items found in OneDrive share (or insufficient permissions)')
        return

    for it in items:
        name = it.get('name')
        click.echo(f"Found: {name}")
        download_url = it.get('@microsoft.graph.downloadUrl')
        if not download_url:
            # Fall back to content endpoint for the item id
            item_id = it.get('id')
            if item_id:
                download_url = f'https://graph.microsoft.com/v1.0/shares/{share_id}/driveItem/items/{item_id}/content'
        if not download_url:
            click.echo(f"No download URL for {name}; skipping")
            continue

        if name.lower().endswith('.json'):
            tf = download_to_temp(download_url, suffix='.json', headers=headers if download_url.startswith('https://graph.microsoft.com') else None, timeout=timeout)
            try:
                with open(tf, 'r', encoding='utf-8') as mf:
                    metadata = json.load(mf)
                pid = bl.create_dataset(metadata, dataverse_alias)
                if pid:
                    bl.delete_all_files_in_dataset(pid)
            finally:
                try:
                    os.remove(tf)
                except Exception:
                    pass
        elif name.lower().endswith('.zip'):
            tf = download_to_temp(download_url, suffix='.zip', headers=headers if download_url.startswith('https://graph.microsoft.com') else None, timeout=timeout)
            # same caveat as Google Drive: we create a placeholder dataset if needed
            metadata = {"datasetVersion": {"metadataBlocks": {}}}
            pid = bl.create_dataset(metadata, dataverse_alias)
            if pid:
                bl.delete_all_files_in_dataset(pid)
                bl.upload_zip_file(pid, tf)
            try:
                os.remove(tf)
            except Exception:
                pass
    


@click.command()
@click.option("--remote-list", required=True, help="Path or URL to JSON list of datasets")
@click.option("--api-token", envvar='DATAVERSE_API_TOKEN', help='Dataverse API token')
@click.option("--base-url", default='https://demo.borealisdata.ca', help='Dataverse base URL')
@click.option("--dataverse-alias", default='root', help='Dataverse alias for dataset upload')
@click.option("--onedrive-token", default=None, help='Microsoft Graph Bearer token for OneDrive/SharePoint')
@click.option("--gdrive-token", default=None, help='Google Drive OAuth2 Bearer token')
@click.option("--timeout", default=60, help="HTTP timeout seconds for downloads")
def main(remote_list, api_token, base_url, dataverse_alias, onedrive_token, gdrive_token, timeout):
    """Process remote list and upload datasets/files using BulkLoad_Python helpers."""
    if not api_token:
        raise click.ClickException("API token is required (set DATAVERSE_API_TOKEN or --api-token)")

    # Initialize api in the imported module so its functions use the provided values
    bl.API_TOKEN = api_token
    bl.BASE_URL = base_url
    bl.DATAVERSE_ALIAS = dataverse_alias
    bl.api = NativeApi(base_url, api_token)

    # Load remote_list (URL or local file)
    if remote_list.startswith(("http://", "https://")):
        # Auto-detect provider from URL
        lower = remote_list.lower()
        # OneDrive/SharePoint patterns
        if 'sharepoint' in lower or '1drv.ms' in lower or 'onedrive' in lower:
            if not click.get_current_context().params.get('onedrive_token'):
                raise click.ClickException('OneDrive URL detected — please provide --onedrive-token')
            click.echo(f"Detected OneDrive/SharePoint URL: processing via Microsoft Graph")
            process_onedrive_share(remote_list, click.get_current_context().params.get('onedrive_token'), base_url, dataverse_alias, timeout=timeout)
            return
        # Google Drive patterns
        if 'drive.google.com' in lower:
            if not click.get_current_context().params.get('gdrive_token'):
                raise click.ClickException('Google Drive URL detected — please provide --gdrive-token')
            click.echo(f"Detected Google Drive URL: processing via Drive API")
            process_google_drive_folder(remote_list, click.get_current_context().params.get('gdrive_token'), base_url, dataverse_alias, timeout=timeout)
            return

        resp = requests.get(remote_list, timeout=timeout)
        resp.raise_for_status()
        # Auto-detect HTML directory index pages: if content-type is HTML
        # or the body contains an <html> tag, treat it as a folder listing
        ctype = resp.headers.get('content-type', '').lower()
        body = resp.text or ''
        if 'text/html' in ctype or '<html' in body.lower():
            click.echo(f"Detected HTML index at {remote_list}; processing as folder listing")
            process_remote_folder_url(remote_list, api_token, base_url, dataverse_alias, timeout=timeout)
            return
        # Otherwise assume it's JSON
        items = resp.json()
    else:
        with open(remote_list, 'r', encoding='utf-8') as f:
            items = json.load(f)

    if not isinstance(items, list):
        raise click.ClickException("remote-list JSON must be an array of entries")

    for i, entry in enumerate(items, start=1):
        click.echo(f"\n--- Processing entry {i}/{len(items)} ---")
        # Determine metadata
        metadata = None
        metadata_temp = None
        file_temp = None
        try:
            if 'metadata' in entry:
                metadata = entry['metadata']
            elif 'metadata_url' in entry:
                m_url = entry['metadata_url']
                # download metadata file (assume JSON)
                metadata_temp = download_to_temp(m_url, suffix='.json', timeout=timeout)
                with open(metadata_temp, 'r', encoding='utf-8') as mf:
                    metadata = json.load(mf)
            else:
                raise click.ClickException('Entry must include either "metadata" or "metadata_url"')

            # Create dataset
            pid = bl.create_dataset(metadata, dataverse_alias)
            if not pid:
                click.echo('✗ create_dataset failed; skipping this entry')
                continue

            # Always attempt to delete existing files (keeps behavior consistent)
            try:
                bl.delete_all_files_in_dataset(pid)
            except Exception as e:
                click.echo(f"⚠ Warning: delete_all_files_in_dataset failed: {e}")

            # Handle file upload if provided
            if 'file_url' in entry and entry['file_url']:
                f_url = entry['file_url']
                file_temp = download_to_temp(f_url, suffix=Path(f_url).suffix or '.dat', timeout=timeout)
                success = bl.upload_zip_file(pid, file_temp)
                if not success:
                    click.echo('✗ upload failed for this entry')
            else:
                click.echo('ℹ No file_url provided; dataset created without files')

        except Exception as exc:
            click.echo(f"✗ Error processing entry {i}: {exc}")
        finally:
            # cleanup temp files
            for tmp in (metadata_temp, file_temp):
                if tmp:
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass


if __name__ == '__main__':
    main()

'''
python remote_loader.py \
  --remote-list "https://utoronto-my.sharepoint.com/:f:/g/personal/zeynep_cevik_utoronto_ca/Egdcqzq6wDhCqcGl66b2x1UB7xnIbqH9Dj7UdeTlqZkxWA?e=7AHvCG" \
  --base-url "https://demo.borealisdata.ca" \
  --dataverse-alias "zeynepcevik"
  '''
#Run with a OneDrive / SharePoint share URL
#Replace YOUR_GRAPH_TOKEN with a Microsoft Graph Bearer token that has permission to read the shared items (short-lived token from Graph Explorer or an app token with appropriate scopes).
'''
python3 remote_loader.py \
  --remote-list "https://utoronto-my.sharepoint.com/:f:/g/personal/zeynep_cevik_utoronto_ca/Egdcqzq6wDhCqcGl66b2x1UB7xnIbqH9Dj7UdeTlqZkxWA?e=7AHvCG" \
  --onedrive-token "YOUR_GRAPH_TOKEN" \
  --base-url "https://demo.borealisdata.ca" \
  --dataverse-alias "zeynepcevik"
  '''

#Run with a Google Drive folder URL
#Replace YOUR_GDRIVE_OAUTH_TOKEN with an OAuth2 access token that includes Drive scopes (e.g. https://www.googleapis.com/auth/drive.readonly).
'''
python3 remote_loader.py \
  --remote-list "https://drive.google.com/drive/folders/<FOLDER_ID>" \
  --gdrive-token "YOUR_GDRIVE_OAUTH_TOKEN" \
  --base-url "https://demo.borealisdata.ca" \
  --dataverse-alias "zeynepcevik"
    '''

'''Notes and safety

The script will create datasets and then call delete_all_files_in_dataset for each dataset (this removes existing files). Test against a sandbox dataverse or use a test dataverse alias first.
Tokens:
OneDrive/SharePoint: obtain a Microsoft Graph bearer token (Graph Explorer for quick tests or an app/client credentials flow). Scopes: Files.Read.All / Sites.Read.All or delegated scopes that let you access the share.
Google Drive: obtain an OAuth2 access token with Drive read scopes. For quick testing you can use gcloud auth application-default print-access-token if your account has access to the Drive folder.
If you prefer not to expose tokens on the command line, store them in environment variables and pass them in (example below).
If you want, I can:

Run a quick local syntax/lint check (I can run python -m py_compile remote_loader.py for you).
Add a --no-delete flag to prevent the automatic deletion step while you test. Which would you prefer?
GPT-5 mini • 1x
'''
