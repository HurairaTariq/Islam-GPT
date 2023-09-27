import pandas as pd
import PyPDF2
import os
from sentence_transformers import SentenceTransformer
from elasticsearch import Elasticsearch, exceptions as es_exceptions
import openai
from openai import Embedding




trans2 = 'No Verse Available'
EMBEDDING_DIMS = 768
ENCODER_BOOST = 10
books = [] 

sentence_transformer = SentenceTransformer("sentence-transformers/multi-qa-mpnet-base-dot-v1")


es_client = Elasticsearch("http://localhost:9200", verify_certs=False, request_timeout=60)
#  es_client = Elasticsearch("http://elasticsearch:9200", verify_certs=False, request_timeout=600)

def get_companies():
    index_name = 'elasticbook'
    try:
        companies = set()
        result = es_client.search(
            index=index_name, 
            body={"query": {"match_all": {}}},
            scroll='1m'
        )
        scroll_id = result['_scroll_id']
        hits = result['hits']['hits']
        while len(hits) > 0:
            for hit in hits:
                companies.add(hit['_source']['book'])
            result = es_client.scroll(scroll_id=scroll_id, scroll='1m')
            scroll_id = result['_scroll_id']
            hits = result['hits']['hits']
        return companies
    except:
        return []


def create_index(index_name) -> None:
    es_client.options(ignore_status=404).indices.delete(index=index_name)
    es_client.options(ignore_status=400).indices.create(
        index=index_name,
        mappings={
            "properties": {
                "embedding": {
                    "type": "dense_vector",
                    "dims": EMBEDDING_DIMS,
                },
                "context": {
                    "type": "text",
                },
                "company": {
                    "type": "text",
                },
                "chvr": {
                    "type": "text",
                },
                "trans1": {
                    "type": "text",
                },
            }
        }
    )

def refresh_index(index_name) -> None:
    es_client.indices.refresh(index=index_name)

# Add values to the database 
def index_context(index_name, chvr, cname, trans, contexts):
    for count, items in enumerate(chvr):
        context1 = contexts[count]
        chvr1 = chvr[count]
        print(chvr1)
        company1 = cname[count]
        trans1 = trans[count]
        embedding = sentence_transformer.encode(context1)
        data = {
            "context": context1,
            "embedding": embedding,
            "company": company1,
            "chvr": chvr1,
            "trans1": trans1,
        }
        
        es_client.options(max_retries=0).index(
            index=index_name,
            document=data
        )


def text_data(excel):
    # excel_file = 'TAZKIRUL QURAN PROJECT.xlsx'
    df = pd.read_excel(excel)
    # print(df.columns)
    verse = []
    chapter = []
    cname = []
    trans = []
    chvr = []
    commentary = []
    sentence_chunck = ''
    for index, row in df.iterrows():
        # if row['Verse Number'] != 0:
            # if index == 16:
            #     break
            if not commentary:
                print(index)
                chapter.append(row['Chapter Number'])
                verse.append(row['Verse Number'])
                cname.append(row['Chapter Name'])
                commentary.append(row[' Commentary'])
                sentence_chunck += row['Latest (English) Translation']
                
            elif row['Verse Number'] == 0:
                print('done')
                # commentary.clear()
                # trans.clear()
            elif commentary[-1] == row[' Commentary']:
                verse.append(row['Verse Number'])
                sentence_chunck += row['Latest (English) Translation']
                # trans.append(row['Latest (English) Translation'])
                print(index)
            elif commentary[-1] != row[' Commentary']:
                print(index)
                a = str(verse[0])
                b = str(verse[-1])
                c = str(chapter[-1])
                x = 'Chapter #' + c + ', Verse ' + a + '-' + b 
                verse.clear()
                chapter.append(row['Chapter Number'])
                verse.append(row['Verse Number'])
                chvr.append(x)
                trans.append(sentence_chunck)
                sentence_chunck = ''
                sentence_chunck += row['Latest (English) Translation']
                commentary.append(row[' Commentary'])
                cname.append(row['Chapter Name'])
                # print(trans)
                # print(cname)
                # print(chvr)
                
                # commentary.clear()
                # trans.clear()
                # trans.append(row['Latest (English) Translation'])
    a = str(verse[0])
    b = str(verse[-1])
    c = str(chapter[-1])
    x = 'Chapter #' + c + ', Verse ' + a + '-' + b 
    verse.clear()
    chapter.append(row['Chapter Number'])
    verse.append(row['Verse Number'])
    chvr.append(x)
    trans.append(sentence_chunck)
                
    # print(trans)
    # print(cname)
    # print(chvr)
    retrieval1(chvr, cname, trans, commentary)
    # print (df.head(10))

#books code
def textdatab():
  base_path = 'books'               
  author = 'Maulana Wahiduddin Khan'                          
  for name in os.listdir(base_path):                     
    print(name)
    book_name = name.split('.')[0].split('_')[1]                      
    book = { 'Author':author, 'Book Name':book_name, 'Content':[] }                  
    file_path = os.path.join(base_path,name)                               
    pdfFileObj = open(file_path, 'rb')                    
    pdfReader = PyPDF2.PdfReader(pdfFileObj)                         

    add = False                               
    temp = {'page':'', 'chapter':'', 'content':''}                     
    for num_p,page in enumerate(pdfReader.pages[1:]):                                   

      text = page.extract_text()                                
      split_text = text.split('\n')                
      text_length = len(split_text)                
      text_length_flag = text_length>3                           
      page_length = len(pdfReader.pages)                             
      if text_length_flag:                      
        chap = " ".join(str(split_text[1]).lower().strip().split())                 
        chap_2 = " ".join(str(split_text[2]).lower().strip().split())                 
        bk = " ".join(str(split_text[0]).lower().strip().split())                 

      if (text_length_flag):                     
        if ( (chap!='introduction') and (chap!='table of contents') and (chap!='foreword') and (chap!='table of content') and (chap!='preface') and (chap!='forward') and (chap!='publisher’s note') ):                    
          add = True                  
        else:               
          continue                 
      if (text_length_flag):             
        if ( ((num_p>(page_length*(3/4)))and(bk==chap)) or ((num_p>(page_length*(3/4)))and(chap in bk)and(chap_2 in bk)) or ((num_p>(page_length*(3/4)))and(chap=='a final word')) or (('conclusion'in chap)and(num_p>(page_length*(3/4)))) or (chap=='notes') or (chap=='index') or (chap=='in search of god') or (chap=='the callof the quran') ):                          
          break                    
      if ( add and (text_length_flag) ):                                  
        temp['page'] = str(int(num_p+2))                   
        temp['chapter'] = chap                         
                                      
        for num,txt in enumerate(split_text[3:]):                  
          a = " ".join(str(txt).strip().split())                     
          a1 = ''                                             
          for char in txt:                         
            if char.isalpha():                 
              a1 += char                           
          if a!='' and a!=' ' and not(a1.isupper()):                     
            if not( (num==1 or num==2) and ( (" ".join(str(txt).lower().strip().split())) in temp['chapter'] ) ):  
              temp['content'] += a+' '                                                              
                                                
        if len(temp['content']):                               
          book['Content'].append(temp)                        
        temp = {'page':'', 'chapter':'', 'content':''}                       
    
    books.append(book)
    

def textdatab2():
    base_path = 'books'                                                                     
    author = 'Maulana Wahiduddin Khan'                                              
    for name in os.listdir(base_path):                                              
      print(name)
      book_name = name.split('.')[0].split('_')[1]                                  
      book = { 'Author':author, 'Book Name':book_name, 'Content':[] }               
      file_path = os.path.join(base_path,name)                                      
      pdfFileObj = open(file_path, 'rb')                                            
      pdfReader = PyPDF2.PdfReader(pdfFileObj)                                      

      add = False                                                                   
      pg = 0                                                                        


      for num_p,page in enumerate(pdfReader.pages[1:]):                             

        temp = {'page':'', 'chapter':'', 'content':''}                              
        text = page.extract_text()                                                  
        split_text = text.split('\n')                                               
        for i in split_text:                                                        
          i = " ".join(str(i).strip().split())                                      
          if i!='' and i!=' ':                                                      
            test = i                                                                
        if test == 'START':                                                         
          add = True                                                                
          pg = num_p                                                                
        if test == 'END':                                                           
          add = False                                                               

        if add and test != 'START':                                                 
          
          chap = " ".join(str(split_text[6]).strip().split())                       
          temp['page'] = str(num_p+1)                                                
          temp['chapter'] = chap                                                    

          for num,txt in enumerate(split_text[7:]):                  
            a = " ".join(str(txt).strip().split())                     
            a1 = ''                                             
            for char in txt:                         
              if char.isalpha():                 
                a1 += char                           
            if  a!='' and a!=' ' and not(a1.isupper()):                     
              if not( (num==1 or num==2) and ( (" ".join(str(txt).lower().strip().split())) in temp['chapter'] ) ):  
                temp['content'] += a+' '                                                              
                                                  
          if len(temp['content']):                               
            book['Content'].append(temp)                        
          temp = {'page':'', 'chapter':'', 'content':''}                            

      books.append(book)


def textdatab3():
    months = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 'JULY',       
              'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER', 'SEPTMBER'] 
    author = 'Maulana Wahiduddin Khan'                                              
    book_name = 'Simple-Wisdom'                                                     
    book = { 'Author':author, 'Book Name':book_name, 'Content':[] }                 
    file_path = 'books/01_Simple-Wisdom.pdf'

    pdfFileObj = open(file_path, 'rb')                                              
    pdfReader = PyPDF2.PdfReader(pdfFileObj)                                        
                                                                                    
    add = False                                                                     
    pg = 0                                                                          

    for num_p,page in enumerate(pdfReader.pages[1:]):                               
      temp = {'page':'', 'chapter':'', 'content':''}                                
      if add and num_p>pg+1:                                                        
        temp['chapter'] = book['Content'][-1]['chapter']                            
      text = page.extract_text()                                                    
      split_text = text.split('\n')                                                 
      for i in split_text:                                                          
        i = " ".join(str(i).strip().split())                                        
        if i!='' and i!=' ':                                                        
          test = i                                                                  
      if test == 'START':                                                           
        add = True                                                                  
        pg = num_p                                                                  
      if test == 'END':                                                             
        add = False                                                                 
      if add and test != 'START':                                                   
        temp['page'] = " ".join(str(split_text[0]).strip().split())                 
        title = 0                                                                   
        for sub_text in split_text[1:]:                                             
          a = (" ".join(str(sub_text).strip().split())).split(' ')                  
          if len(a)==2 and a[0] in months:                                          
            if len(temp['content']) and not(temp['content'].isspace()):             
              book['Content'].append(temp)                                          
              temp = {'page':'', 'chapter':'', 'content':''}                        
              temp['page'] = " ".join(str(split_text[0]).strip().split())           
            title = 1                                                               
            temp['chapter'] = sub_text                                              
          elif title==1:                                                            
            temp['chapter'] = temp['chapter'] + ' ' + sub_text                      
            title = 2                                                               
          elif title==2 and len(sub_text)<35 and sub_text!='' and sub_text!=' ':    
            temp['chapter'] = temp['chapter'] + ' ' + sub_text                      
            title = 0                                                               
          else:                                                                     
            title = 0                                                               
            temp['content'] = temp['content'] + ' ' + sub_text                      
      if add and temp['content']!='' and not(temp['content'].isspace()):            
        book['Content'].append(temp)
    books.append(book)

def textdatab4():
  base_path = 'books'                                                                      
  author = 'Maulana Wahiduddin Khan'                                              
  for name in os.listdir(base_path):                                              
    print(name)
    if not(name.endswith('.pdf')):                                                
      continue                                                                    
    book_name = name.split('.')[0].split('_')[1]                                  
    book = { 'Author':author, 'Book Name':book_name, 'Content':[] }               
    file_path = os.path.join(base_path,name)                                      
    pdfFileObj = open(file_path, 'rb')                                            
    pdfReader = PyPDF2.PdfReader(pdfFileObj)                                      
                                                                                  
    add = False                                                                   
                                                                                  
    for num_p,page in enumerate(pdfReader.pages[1:]):                             

      temp = {'page':'', 'chapter':'', 'content':''}                               
      text = page.extract_text()                                                  
      split_text = text.split('\n')                                               

      if len(text) == 13:                                                         
        add = True                                                                
        continue                                                                  
      elif len(text) == 11:                                                       
        break                                                                     
      
      if add:                                                                     
        temp['page'] = str(num_p)                                                 
        temp['chapter'] = book_name.replace('-',' ')                              
        for num,txt in enumerate(split_text):                                    
          if txt == '' or txt.isspace():                                        
            continue                                                              
          a = " ".join(str(txt).strip().split())                                 
          temp['content'] += a + ' '                                              
        if len(temp['content']):                                                  
          book['Content'].append(temp)                                            
        temp = {'page':'', 'chapter':'', 'content':''}                            
    books.append(book)

  print(books)

def textdatab5():

  base_path = 'books'                                                       
  author = 'Maulana Wahiduddin Khan'                                              
  for name in os.listdir(base_path):                                              
    print(name)
    if not(name.endswith('.pdf')):                                                
      continue                                                                    
    book_name = name.split('.')[0].split('_')[1]                                  
    book = { 'Author':author, 'Book Name':book_name, 'Content':[] }               
    file_path = os.path.join(base_path,name)                                      
    pdfFileObj = open(file_path, 'rb')                                            
    pdfReader = PyPDF2.PdfReader(pdfFileObj)                                      
                                                                                  
    add = False                                                                   
                                                                                  
    temp = {'page':'', 'chapter':'', 'content':''}                                
    chap = ''                                                                     
                                                                                  
    for num_p,page in enumerate(pdfReader.pages[1:]):                             
      temp = {'page':'', 'chapter':'', 'content':''}                              
      text = page.extract_text()                                                  
      split_text = text.split('\n')                                               

      if len(text) == 13:                                                         
        add = True                                                                
        continue                                                                  
      elif len(text) == 11:                                                       
        break                                                                      
      
      if add:                                                                     
        temp['page'] = str(num_p)                                                 
        temp['chapter'] = chap                                                    
        for num,txt in enumerate(split_text):                                     
          if txt == '' or txt.isspace():                                          
            continue                                                              
          a = " ".join(str(txt).strip().split())                                  
          a1 = ''                                                                 
          for ch in txt:                                                          
            if ch.isalpha():                                                      
              a1 += ch                                                            
          if a1.isupper():                                                        
            if len(temp['content']):                                              
              book['Content'].append(temp)                                        
              temp = {'page':'', 'chapter':'', 'content':''}                      
            temp['chapter'] = a                                                   
            temp['page'] = str(num_p)                                             
            chap = a                                                              
            continue                                                              
          temp['content'] += a + ' '                                              
        if len(temp['content']):                                                
          book['Content'].append(temp)                                            
          temp = {'page':'', 'chapter':'', 'content':''}                          
    books.append(book)
    


def textdatab6():
  base_path = 'books' 
  author = 'Maulana Wahiduddin Khan'                                              
  for name in os.listdir(base_path):
    print(name)                                          
    if not(name.endswith('.pdf')):                                                
      continue                                                                    
    book_name = name.split('.')[0].split('_')[1]                                  
    book = { 'Author':author, 'Book Name':book_name, 'Content':[] }               
    file_path = os.path.join(base_path,name)                                      
    pdfFileObj = open(file_path, 'rb')                                            
    pdfReader = PyPDF2.PdfReader(pdfFileObj)                                      
                                                                                  
    add = False                                                                   
                                                                                  
    for num_p,page in enumerate(pdfReader.pages[1:]):                             

      temp = {'page':'', 'chapter':'', 'content':''}                              
      text = page.extract_text()                                                  
      split_text = text.split('\n')                                               

      if len(text) == 13:                                                         
        add = True                                                                
        continue                                                                  
      elif len(text) == 11:                                                       
        break                                                                     
      
      if add:                                                                     
        temp['page'] = str(num_p)                                                 
        temp['chapter'] = book_name.replace('-',' ')                              
        for num,txt in enumerate(split_text):                                    
          if txt == '' or txt.isspace():                                        
            continue                                                              
          a = " ".join(str(txt).strip().split())                                 
          temp['content'] += a + ' '                                              
        if len(temp['content']):                                                  
          book['Content'].append(temp)                                            
        temp = {'page':'', 'chapter':'', 'content':''}                            
    books.append(book)
      
      

def create_index1(index_name) -> None:
    es_client.options(ignore_status=404).indices.delete(index=index_name)
    es_client.options(ignore_status=400).indices.create(
        index=index_name,
        mappings={
            "properties": {
                "embedding": {
                    "type": "dense_vector",
                    "dims": EMBEDDING_DIMS,
                },
                "auther": {
                    "type": "text",
                },
                "book": {
                    "type": "text",
                },
                "page": {
                    "type": "text",
                },
                "chapter": {
                    "type": "text",
                },
                "content": {
                    "type": "text",
                },
            }
        }
    )

def refresh_index1(index_name) -> None:
    es_client.indices.refresh(index=index_name)

# Add values to the database 
def index_context1(index_name):
    for book in books:
        print("inserting: ", book["Book Name"])
        Auther = book["Author"]
        BookName = book["Book Name"]
        print(BookName)
        for content in book["Content"]:
            Page = content["page"]
            Chapter = content["chapter"]
            print(Chapter)
            Content = content["content"]
        embedding = sentence_transformer.encode(Content)
        data = {
            "embedding": embedding,
            "auther": Auther,
            "book": BookName,
            "page": Page,
            "chapter": Chapter,
            "content": Content,
        }
        
        es_client.options(max_retries=0).index(
            index=index_name,
            document=data
        )


def retrieval1(chvr, cname, trans, commentary):
    if not es_client.indices.exists(index="elasticdb"):
        create_index("elasticdb")
        print("creating index")
    print("refreshing index")
    refresh_index("elasticdb")
    print("inserting: ",cname)
    index_context("elasticdb", chvr, cname, trans, commentary)
    print("inserted")

def retrievalb():
    if not es_client.indices.exists(index="elasticbook"):
        create_index1("elasticbook")
        print("creating index")
    print("refreshing index")
    refresh_index1("elasticbook")
    index_context1("elasticbook")
    books.clear()
    print("inserted")



def retrieval(excel):
    text_data(excel)

def mainbook():
   textdatab()
   retrievalb()

def mainbook2():
   textdatab2()
   retrievalb()

def mainbook3():
   textdatab3()
   retrievalb()

def mainbook4():
   textdatab4()
   retrievalb()

def mainbook5():
   textdatab5()
   retrievalb()

def mainbook6():
   textdatab6()
   retrievalb()