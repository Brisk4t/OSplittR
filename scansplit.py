import ocrmypdf
import os
from pikepdf import Pdf
from multiprocessing import Pool, cpu_count, Process
import time 
import psutil
import PyPDF2
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
                expression = "[\n\r].*"+ String + "\s*([^\n\r]*)"
                #print(expression)
                tmp = re.search(expression, Text, re.IGNORECASE)[1]
                name = re.sub(r'[^A-Za-z0-9 ]+', '', tmp)
                name = name.strip()
                if((name in err_names) or name == None):
                    name = "Err"
                
                return name

        

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


def split_ocr(file_SRC, DST_dir, tmp_dir):
    filename = file_SRC.split(sep='/')[-1]
    temp_file = tmp_dir + "/" + filename
   
    Search_string1 = "Legal Entity Name"
    Search_string2 = "Proponent’s Legal Name:"
    Cores_Start_String = "Corporate Registration System"
    Cores_End_String = "This is to certify that,"
    
    ocrmypdf.ocr(file_SRC, temp_file, force_ocr=True)

    enname = search(temp_file, Search_string1, "string")
    if(enname == None or enname.strip() == None or enname.strip() == ""):
        enname = search(temp_file, Search_string2, "string")
        if(enname == None or enname.strip() == None or enname.strip() == ""):
            enname = "Error"
     
    dirname = DST_dir + "/" + enname


    if(not(os.path.isdir(dirname))): # If directory does not exist
        os.mkdir(dirname) # Create a new directory of entity name

        if(enname == "Error"): # If entity is error
            new_dir = dirname + "/" + "0" 
            os.mkdir(new_dir) # Create a first folder for the list
            dirname = new_dir # Assign the new folder as the working directory



    else: # If directory already exists
        folders = [x for x in os.listdir(dirname) if os.path.isdir(dirname + "/" + x)] # Get all files in directory
        folders.sort()
        
        if not folders: # If there are no folders in the directory
            new_index = "0" # Create a new folder '1'
        
        else: # if there are other folders in the directory
            new_index = folders[-1] # Get the last folder
        
        new_dir_name = str(int(new_index) + 1) # Create a folder name by incrementing the last existing folder

        new_dir = dirname + "/" + new_dir_name
        os.mkdir(new_dir)
        dirname = new_dir

    DST_file = dirname + "/"+ enname + ".pdf"
    shutil.copyfile(temp_file, DST_file)

    cores_start = search(DST_file, Cores_Start_String, "page") 
    cores_end = search(DST_file, Cores_End_String, "page")

    print("Cores Start:", cores_start)
    print("Cores End:", cores_end)

    os.remove(file_SRC)

    if(not (cores_start == None or cores_end == None)):
        split(DST_file, cores_start, cores_end)

    else:
        return



def batch_target(log, DST, TMP, doc):
    try:
       split_ocr(doc, DST, TMP)

    except Exception as excpt:
        log.error(excpt)

def limit_cpu():
    p = psutil.Process(os.getpid())
    # set to lowest priority, this is windows only, on Unix use ps.nice(19)
    p.nice(psutil.BELOW_NORMAL_PRIORITY_CLASS)

def batchocr(log, SRC, DST, TMP): # OCR every file in SRC and save to DST
    pool = Pool(None, limit_cpu) # Use max no of processes = threads

    docs = []
    for doc in os.listdir(SRC):    
        source = SRC + "/" + doc
        docs.append(source)

    #print(docs)

    func = partial(batch_target, log, DST, TMP)

    for p in pool.imap(func, docs):
        pass



if __name__=="__main__":

    completed_folder = "CMPL"

    if(not(os.path.isdir(completed_folder))):
        os.mkdir(completed_folder)

    SRC = input("Enter source PDF path: ")
    DST = input("Enter path for output: ")


    log = DST + "/logs.txt"
    logging.basicConfig(filename=log, level=logging.DEBUG, 
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
    
    logger=logging.getLogger(__name__)

    start = time.time()

    batchocr(logger, SRC, DST, completed_folder)
    
    end = time.time()
    print("Elapsed:", end-start)







