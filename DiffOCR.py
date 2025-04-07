import tempfile
import ocrmypdf
import os
import shutil
from pathlib import Path
import glob
from dateutil import parser
import fitz
import logging
import re


def differential_hocr(pdf_file, output_file, preprocessor_function, *args, **kwargs):
    """Run a custom preprocessor on a pdf file and then OCR it, then merge the hOCR output with the original
    Args:
        pdf_file (str): The file to OCR
        output_file (str): Destination file path (with extension)
        preprocessor_function (function): Preprocessor function to run on pdf_file

    Returns:
        str: The path to the output pdf (same as arg2)

    """
    # Create temp folders for the preprocessed and original pdfs
    hocr_tmp_folder = Path(tempfile.mkdtemp(prefix='ocrmypdf.io.'))
    optimized_tmp_folder = Path(tempfile.mkdtemp(prefix='ocrmypdf.io.'))

    # Call the preprocessor
    preprocessed_pdf = preprocessor_function(pdf_file, *args, **kwargs)

    # Get the hocr directory for both pdfs 
    ocrmypdf.pdf_to_hocr(preprocessed_pdf, hocr_tmp_folder, force_ocr=True, image_dpi=400) # OCR the preprocessed pdf
    ocrmypdf.pdf_to_hocr(pdf_file, optimized_tmp_folder, force_ocr=True, tesseract_timeout=0, image_dpi=400) # Apply all non-ocr functions to original pdf

    # Copy the hOCR data from the preprocessed file folder to the original file folder
    files_to_copy = glob.glob(os.path.join(hocr_tmp_folder, "*_hocr.txt")) # Get the list of hocr files from the preprocessed file
    files_to_copy = glob.glob(os.path.join(hocr_tmp_folder, "*_hocr.hocr")) # Get the list of hocr files from the preprocessed file

    for file in files_to_copy:
        shutil.copy(file, optimized_tmp_folder) # Copy the hocr data, replacing the placeholder files generated during image processing

    # Construct the final pdf
    ocrmypdf.hocr_to_ocr_pdf(optimized_tmp_folder, output_file)

    return output_file

def extract_text_from_pdf(pdf_path):
    """Extracts text from a PDF file using fitz."""
    try:
        doc = fitz.open(pdf_path)
        full_text = ""
        
        # Loop through each page and extract text
        for page_num in range(len(doc)):
            page = doc.load_page(page_num)  # Load the page
            full_text += page.get_text()  # Extract text from the page

        return full_text
    
    except Exception as e:
        logging.error(f"Error extracting text: {e}")
        return None
    
def extract_valid_dates(text):
    """Find and validate dates in a given text block."""
    
    # Regex patterns for common date formats
    date_patterns = [
        r'\b\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}\b',  # DD/MM/YYYY, MM-DD-YYYY, DD.MM.YYYY
        r'\b\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\b',  # YYYY-MM-DD
        r'\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4}\b',  # Month DD, YYYY
        r'\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}\b'  # DD Month YYYY
    ]

    # Combine patterns
    combined_pattern = '|'.join(date_patterns)
    
    # Find potential dates
    potential_dates = re.findall(combined_pattern, text, re.IGNORECASE)

    valid_dates = []
    for date_str in potential_dates:
        try:
            parsed_date = parser.parse(date_str, fuzzy=False)  # Validate
            formatted_date = parsed_date.strftime("%y%m%d")  # Convert to YYMMDD
            valid_dates.append(formatted_date)

        except (ValueError, OverflowError):
            pass  # Ignore invalid dates

    if valid_dates:
        return valid_dates[0]
    
    return None

def copy_file_to_dir(src_file: str, dest_dir: str):
    """
    Copies a file to a specified directory. Creates the directory if it doesn't exist.

    Args:
        src_file (str): The path of the source file.
        dest_dir (str): The destination directory.

    Returns:
        str: The path of the copied file.
    
    Raises:
        FileNotFoundError: If the source file does not exist.
    """
    if not os.path.isfile(src_file):
        raise FileNotFoundError(f"Source file '{src_file}' not found.")

    os.makedirs(dest_dir, exist_ok=True)  # Create directory if it doesn't exist
    dest_file = os.path.join(dest_dir, os.path.basename(src_file))
    shutil.copy2(src_file, dest_file)  # Copy file with metadata

    return dest_file

def grayscale_preprocess(pdf_path):
    """Turn the given pdf into grayscale
    
    Args:
        pdf_path (str): Path to the color pdf file

    Returns:
        str: Path to processed_pdf
    """
    doc = fitz.open(pdf_path)
    
    new_doc_path = tempfile.gettempdir() + os.path.basename(pdf_path)
    new_doc = fitz.open()  # Create a new PDF

    for page in doc:
        # Render the page as a grayscale image
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), colorspace=fitz.csGRAY)

        # Create a new page with the same dimensions
        new_page = new_doc.new_page(width=page.rect.width, height=page.rect.height)

        # Insert the grayscale image as the new page content
        new_page.insert_image(new_page.rect, pixmap=pix)

    # Save to memory
    new_doc.save(new_doc_path)
    return new_doc_path







