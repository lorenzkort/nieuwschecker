#!/usr/bin/env python3
"""
Upload HTML files to Strato server using SFTP (Paramiko)
"""
import dagster as dg
import paramiko
import os

logging = dg.get_dagster_logger()

class StratoUploader:
    """Handle SFTP uploads to Strato server"""
    
    def __init__(self):
        """
        Initialise Strato uploader
        
        Args:
            host: Your Strato server address (e.g., 'your-domain.com' or SSH hostname from Strato)
            username: Your SSH/SFTP username
            password: Your SSH/SFTP password
            port: SSH port
        """
        self.host = os.environ.get("FTP_HOST")
        self.username = os.environ.get("FTP_UN")
        self.password = os.environ.get("FTP_PW")
        self.port = int(os.environ.get("FTP_PORT"))
        self.transport = None
        self.sftp = None
    
    def connect(self):
        """Establish SFTP connection"""
        try:
            # Create SSH transport
            self.transport = paramiko.Transport((self.host, self.port))
            self.transport.connect(username=self.username, password=self.password)
            
            # Create SFTP client
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            logging.info(f"✓ Connected to {self.host}")
            return True
            
        except paramiko.AuthenticationException:
            logging.info("✗ Authentication failed. Check your username and password.")
            return False
        except paramiko.SSHException as e:
            logging.info(f"✗ SSH error: {e}")
            return False
        except Exception as e:
            logging.info(f"✗ Connection error: {e}")
            return False
    
    def disconnect(self):
        """Close SFTP connection"""
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()
        logging.info("✓ Disconnected")
    
    def upload_file(self, local_file, remote_path='/', overwrite=False):
        """
        Upload a single file
        
        Args:
            local_file: Path to local file
            remote_path: Destination directory on server (default: '/')
            overwrite: Whether to overwrite existing files (default: False)
        """
        self.connect()
        
        if not self.sftp:
            logging.info("✗ Not connected. Call connect() first.")
            return False
        
        try:
            # Ensure local file exists
            if not os.path.exists(local_file):
                logging.info(f"✗ Local file not found: {local_file}")
                return False
            
            # Get filename
            filename = os.path.basename(local_file)
            
            # Construct remote filepath
            if remote_path.endswith('/'):
                remote_file = remote_path + filename
            else:
                remote_file = f"{remote_path}/{filename}"
            
            # Check if file exists on remote server
            file_exists = False
            try:
                self.sftp.stat(remote_file)
                file_exists = True
            except FileNotFoundError:
                # File doesn't exist
                pass
            
            # Handle existing file based on overwrite flag
            if file_exists:
                if overwrite:
                    # Delete the existing file before uploading
                    try:
                        self.sftp.remove(remote_file)
                        logging.info(f"⚠ Deleted existing file: {remote_file}")
                    except Exception as e:
                        logging.info(f"✗ Failed to delete existing file {remote_file}: {e}")
                        return False
                else:
                    # File exists and overwrite is False
                    logging.info(f"⚠ File already exists: {remote_file} (skipped, overwrite=False)")
                    return False
            
            # Upload file
            self.sftp.put(local_file, remote_file)
            
            # Get file size for confirmation
            file_size = os.path.getsize(local_file)
            action = "Overwritten" if file_exists else "Uploaded"
            logging.info(f"✓ {action}: {filename} ({file_size} bytes) → {remote_file}")
            return True
            
        except Exception as e:
            logging.info(f"✗ Upload failed for {local_file}: {e}")
            return False
        finally:
            self.disconnect()


if __name__ == '__main__':
    """Example usage"""
    
    # Files to upload
    HTML_FILE = 'index.html'  # Change to your file
    REMOTE_PATH = '/'
    # ========================
    
    # Create uploader instance
    uploader = StratoUploader()
    
    # Connect to server
    if uploader.connect():
        # Upload with overwrite (default)
        uploader.upload_file(HTML_FILE, REMOTE_PATH)
        
        # Or upload without overwriting existing files
        # uploader.upload_file(HTML_FILE, REMOTE_PATH, overwrite=False)
        
        uploader.disconnect()
    else:
        logging.info("Failed to connect to server")