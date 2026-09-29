import io
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage

SERVICE_ACCOUNT_FILE = 'service-account-key.json'
DRIVE_FOLDER_ID = ''
GCS_BUCKET_NAME = ''
DELETE_FROM_DRIVE_AFTER_MOVE = True  # Set to False if you only want to copy

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, 
    scopes=[
        'https://googleapis.com',
        'https://googleapis.com'
    ]
)

# Initialize clients
drive_service = build('drive', 'v3', credentials=creds)
storage_client = storage.Client(credentials=creds, project=creds.project_id)
bucket = storage_client.bucket(GCS_BUCKET_NAME)

def move_drive_files_to_gcs():
    # List all files inside the specified Google Drive folder
    query = f"'{DRIVE_FOLDER_ID}' in parents and trashed = false"
    results = drive_service.files().list(
        q=query, 
        fields="nextPageToken, files(id, name, mimeType)"
    ).execute()
    
    files = results.get('files', [])
    
    if not files:
        print("No files found in the specified Google Drive folder.")
        return

    print(f"Found {len(files)} files to process.")

    for file in files:
        file_id = file['id']
        file_name = file['name']
        mime_type = file['mimeType']
        
        # Skip subfolders if they exist inside the folder
        if mime_type == 'application/vnd.google-apps.folder':
            print(f"Skipping folder: {file_name}")
            continue

        print(f"Processing: {file_name}...")

        try:
            request = drive_service.files().get_media(fileId=file_id)
            file_stream = io.BytesIO()
            downloader = MediaIoBaseDownload(file_stream, request)
            
            done = False
            while not done:
                status, done = downloader.next_chunk()
            
            # Reset stream pointer back to the beginning
            file_stream.seek(0)

            # Upload the file stream to Google Cloud Storage
            blob = bucket.blob(file_name)
            blob.upload_from_file(file_stream, content_type=mime_type)
            print(f" Successfully uploaded to GCS: gs://{GCS_BUCKET_NAME}/{file_name}")

            # Optional: Delete the file from Google Drive to complete the "Move"
            if DELETE_FROM_DRIVE_AFTER_MOVE:
                drive_service.files().delete(fileId=file_id).execute()
                print(f"🗑️ Deleted from Google Drive: {file_name}")

        except Exception as e:
            print(f"❌ Error moving file {file_name}: {str(e)}")

if __name__ == '__main__':
    move_drive_files_to_gcs()
