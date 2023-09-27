from flask import Flask, request
from utils import *
import os, glob, shutil
import asyncio
import zipfile

app = Flask(__name__)
trans2 = 'No Verse To Show'
comen2 = 'No Commentary To Show'

@app.route('/api/healthcheck',methods = ["POST"])
def hello():
        # resp = MessagingResponse()
        # resp.message("Hello World!")
        company = get_companies()
        print(company, len(company))
        # return str(resp) 
        return {"msg": 'Hello World!'}

@app.route('/api/verse', methods=["GET"])
def verse():
    async def func():
        global trans2
        return {"Verse": trans2}
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(func())
    loop.close()
    return result

@app.route('/api/commen', methods=["GET"])
def commen():
    async def func():
        global comen2
        return {"Commentary": comen2}
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(func())
    loop.close()
    return result


@app.route('/api/feedbooks', methods=['POST'])
def feedbooks():
    async def func():
        await asyncio.to_thread(mainbook4)
        return 'done'

    zip_file = request.files.get("file")
    afiles = zip_file.filename
    print(afiles)
    if not afiles.endswith('.zip'):
        return "Enter a Zip File"

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        file_names = zip_ref.namelist()
        print(file_names[0])

        if not os.path.isdir(f'./{file_names[0]}'):
            if file_names[0] in os.listdir("."): os.remove(file_names[0])
            if "books" in os.listdir("."):shutil.rmtree("books")
            os.mkdir("books")
            zip_ref.extractall('./books')
            txt_file_names = file_names
        else:
            if file_names[0] in os.listdir("."): os.remove(file_names[0])
            if "books" in os.listdir("."):shutil.rmtree("books")
            zip_ref.extractall('./')  # Extract the entire archive to the current directory
            if file_names[0] != "books":
                os.rename(file_names[0], "books")
            txt_file_names = glob.glob(f'./{file_names[0]}/**/*.pdf', recursive=True) 
            txt_file_names = [f for f in file_names if f.startswith(file_names[0]) and f.endswith('.pdf')]
            txt_file_names = [os.path.basename(f) for f in txt_file_names]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(func())
    loop.close()
    return result

@app.route('/api/feedbooks2', methods=['POST'])
def feedbooks2():
    async def func():
        await asyncio.to_thread(mainbook6)
        return 'done'

    zip_file = request.files.get("file")
    afiles = zip_file.filename
    print(afiles)
    if not afiles.endswith('.zip'):
        return "Enter a Zip File"

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        file_names = zip_ref.namelist()
        print(file_names[0])

        if not os.path.isdir(f'./{file_names[0]}'):
            if file_names[0] in os.listdir("."): os.remove(file_names[0])
            if "books" in os.listdir("."):shutil.rmtree("books")
            os.mkdir("books")
            zip_ref.extractall('./books')
            txt_file_names = file_names
        else:
            if file_names[0] in os.listdir("."): os.remove(file_names[0])
            if "books" in os.listdir("."):shutil.rmtree("books")
            zip_ref.extractall('./')  # Extract the entire archive to the current directory
            if file_names[0] != "books":
                os.rename(file_names[0], "books")
            txt_file_names = glob.glob(f'./{file_names[0]}/**/*.pdf', recursive=True) 
            txt_file_names = [f for f in file_names if f.startswith(file_names[0]) and f.endswith('.pdf')]
            txt_file_names = [os.path.basename(f) for f in txt_file_names]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(func())
    loop.close()
    return result

@app.route('/api/feedbooks3', methods=['POST'])
def feedbooks3():
    async def func():
        await asyncio.to_thread(mainbook5)
        return 'done'

    zip_file = request.files.get("file")
    afiles = zip_file.filename
    print(afiles)
    if not afiles.endswith('.zip'):
        return "Enter a Zip File"

    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        file_names = zip_ref.namelist()
        print(file_names[0])

        if not os.path.isdir(f'./{file_names[0]}'):
            if file_names[0] in os.listdir("."): os.remove(file_names[0])
            if "books" in os.listdir("."):shutil.rmtree("books")
            os.mkdir("books")
            zip_ref.extractall('./books')
            txt_file_names = file_names
        else:
            if file_names[0] in os.listdir("."): os.remove(file_names[0])
            if "books" in os.listdir("."):shutil.rmtree("books")
            zip_ref.extractall('./')  # Extract the entire archive to the current directory
            if file_names[0] != "books":
                os.rename(file_names[0], "books")
            txt_file_names = glob.glob(f'./{file_names[0]}/**/*.pdf', recursive=True) 
            txt_file_names = [f for f in file_names if f.startswith(file_names[0]) and f.endswith('.pdf')]
            txt_file_names = [os.path.basename(f) for f in txt_file_names]

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(func())
    loop.close()
    return result

@app.route('/api/feeddata', methods=['POST'])
def feeddata():
    async def func(afiles):
        await asyncio.to_thread(retrieval, afiles)
        return 'done'

    xfiles = request.files["file"]
    afiles = xfiles.filename
    print(afiles)
    if not afiles.endswith('.xlsx'):
        return "Enter an Excel File"
    if afiles in os.listdir("."):
        os.remove(afiles)
    xfiles.save(os.path.join(os.getcwd(), afiles))
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(func(afiles))
    loop.close()
    return result



if __name__ == '__main__':
    app.run()