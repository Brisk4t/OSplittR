import ocrmypdf
import os
from pikepdf import Pdf
from multiprocessing import Pool, cpu_count, Process
import time 
import psutil
import PyPDF2
import fitz
import logging
import shutil
import re
from functools import partial


err_names = ["Mailing Address", "Name History", "expected"]


def search(file, search, match="page"): # given a pdf file and string, returns page number of first occurence
    object = PyPDF2.PdfReader(file)
    NumPages = len(object.pages) # Get number of pages
    String = search # The string to search for

    # Extract text and do the search
    for i in range(0, NumPages):
        PageObj = object.pages[i]
        Text = PageObj.extract_text()
        if re.search(String,Text, re.IGNORECASE):
            if(match=="page"): # returns page number on which search string was found
                return i
            
            elif(match=="string"): # returns the text until the next newline after string
                expression = r"[\n\r].*" + String + r"\s*([^\n\r]*)"
                #print(expression)
                tmp = re.search(expression, Text, re.IGNORECASE)[1]
                name = re.sub(r'[^A-Za-z0-9 ]+', '', tmp)
                name = name.strip()
                if((name in err_names) or name == None):
                    name = "Err"
                
                return name


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


def find_asset_id(text):
    pattern = r"\b(?:015)?[A-Za-z]{3}\d+\b"
    matches = re.findall(pattern, text)

    # First, prioritize those starting with '015'
    for match in matches:
        if match.startswith('015'):
            return match  # Return the first match starting with '015'

        else:
            return matches[0]
    
    return None


def split (file, start, end): # Given file path, split pdf into 2 files - one in range and one not in rage

    dir_list = file.rsplit(sep='/') # get file path as list
    del dir_list[-1] # keep only file directory path

    dirname = ""
    for item in dir_list:
        dirname = dirname + item + "/"

    cores = dirname + "Cores.pdf"
    submission = dirname + "Submission.pdf"

    pdf = PyPDF2.PdfReader(file)
    pdf_writer1 = PyPDF2.PdfWriter()
    pdf_writer2 = PyPDF2.PdfWriter()

    for page in range(len(pdf.pages)):
        current_page = pdf.pages[page]

        if(page in range(start, end+1)):
            pdf_writer1.add_page(current_page)

        else:
            pdf_writer2.add_page(current_page)
        

    with open(cores, "wb") as out:
        pdf_writer1.write(out)

    with open(submission, "wb") as out:
        pdf_writer2.write(out)

def file_by_assetid(file_path, DST_dir):
    """OCR a source pdf, save a copy with the OCR layer and rename it to asset ID
    
    """
    print(f"Started {file_path}")
    tmp_file = os.path.join(DST_dir, os.path.basename(file_path))
    ocrmypdf.ocr(file_path, tmp_file, force_ocr=True)

    text_data = extract_text_from_pdf(tmp_file)
    asset_id = None
    
    if text_data:
        asset_id = find_asset_id(text_data)


    if asset_id:
        logging.debug(f"Asset ID for {file_path}: {asset_id}")
        filename = os.path.join(DST_dir, asset_id) + ".pdf"
        os.rename(tmp_file, filename)

    return

def batch_target(doc_path, DST):
    try:
       #split_ocr(doc_path, DST, TMP)
       file_by_assetid(doc_path, DST)

    except Exception as excpt:
        logging.error("OCR failed for ", str(doc_path))
        logging.error(excpt)

def limit_cpu():
    p = psutil.Process(os.getpid())
    # set to lowest priority, this is windows only, on Unix use ps.nice(19)
    p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)

def batchocr(SRC, DST): 
    """Loop over SRC to add pdfs to
    :TMP: folder where OCRd PDFs are saved before.. 
    """

    # Create a pool of n processes that each run limit_cpu when started (n = number of threads)
    pool = Pool(processes=None, initializer=limit_cpu)

    # Get the absolute path of each pdf in the SRC directory
    doc_paths = []
    for docname in os.listdir(SRC):    
        doc_path = os.path.join(SRC, docname)
        doc_paths.append(doc_path)

    single_param_batch_target = partial(batch_target, DST=DST)

    # Spawn the worker pool for each file in doc_paths and return each worker as it completes
    results = pool.imap(single_param_batch_target, doc_paths)
    for result in results:
        pass

if __name__=="__main__":
    # Get the input and output pdf directories
    SRC = os.path.abspath(input("Enter a path for a source directory with input PDFs: "))
    DST =  os.path.abspath(input("Enter a path for the output directory: "))

    # Logging (non functional for now)
    log = DST + "/logs.txt"
    logging.basicConfig(filename=log, level=logging.DEBUG, 
                    format="%(asctime)s %(levelname)-8s %(message)s")
    

    start = time.time()

    #file_by_assetid(SRC, DST)

    batchocr(SRC, DST)
    #logging.getLogger().handlers[0].flush()  # Flush file handler to ensure logs are written

    end = time.time()
    print("Elapsed:", end-start)







