"""
Download MoMA CSV files from GitHub
"""
import requests
from pathlib import Path
import sys

# Add project root to Python path so we can import config
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import MOMA_ARTWORKS_URL, MOMA_ARTISTS_URL, FILES

def download_file(url: str, output_path: Path) -> bool:
    """
    Download a file from URL to output_path
    
    Args:
        url: URL to download from
        output_path: Path where file will be saved
        
    Returns:
        True if successful, False otherwise
    """
    try:
        print(f"📥 Downloading from {url}...")
        
        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download with streaming (for large files)
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Write to file
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        # Get file size
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        print(f"✅ Downloaded to {output_path}")
        print(f"   File size: {file_size_mb:.2f} MB")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Network error downloading {url}: {e}")
        raise
    except Exception as e:
        print(f"❌ Error downloading {url}: {e}")
        raise

def download_artworks() -> str:
    """
    Download artworks CSV from GitHub
    
    Returns:
        Path to downloaded file
    """
    print("\n" + "="*60)
    print("Downloading Artworks CSV")
    print("="*60)
    
    success = download_file(MOMA_ARTWORKS_URL, FILES['artworks_csv'])
    if not success:
        raise Exception("Failed to download artworks CSV")
    
    return str(FILES['artworks_csv'])

def download_artists() -> str:
    """
    Download artists CSV from GitHub
    
    Returns:
        Path to downloaded file
    """
    print("\n" + "="*60)
    print("Downloading Artists CSV")
    print("="*60)
    
    success = download_file(MOMA_ARTISTS_URL, FILES['artists_csv'])
    if not success:
        raise Exception("Failed to download artists CSV")
    
    return str(FILES['artists_csv'])

def download_all_files() -> dict:
    """
    Download all MoMA files
    
    Returns:
        Dictionary with file paths
    """
    print("\n" + "="*60)
    print("Downloading All MoMA Files")
    print("="*60)
    
    files = {
        'artworks': download_artworks(),
        'artists': download_artists()
    }
    
    print("\n" + "="*60)
    print("✅ All Downloads Complete!")
    print("="*60)
    print(f"   Artworks: {files['artworks']}")
    print(f"   Artists: {files['artists']}")
    
    return files

# For testing the script directly
if __name__ == "__main__":
    print("Testing download functions...")
    download_all_files()