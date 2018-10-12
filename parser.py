# Prerequisites: python2-rdflib, python2-rdfextras

import rdflib
import mysql.connector
import os.path
#Couple of handy namespaces to use later
RDF    = rdflib.namespace.RDF
RDFN   = rdflib.Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
DCTERM    = rdflib.Namespace("http://purl.org/dc/terms/")
MARCREL   = rdflib.Namespace("http://www.loc.gov/loc.terms/relators/")
PGTERMS = rdflib.Namespace("http://www.gutenberg.org/2009/pgterms/")
CC     = rdflib.Namespace("http://web.resource.org/cc/")
DCAM     = rdflib.Namespace("http://purl.org/dc/dcam/")
BASE = rdflib.Namespace("http://www.gutenberg.org/")


def rdfToTuple(File):

    g = rdflib.Graph()
    g.parse(File)
    #So that we are sure we get something back
    #print "Number of triples",len(g)

    # List all books
    Book_List=[k for k in g.subjects(RDF.type,PGTERMS["ebook"])]
    #Article for wich we want the list of authors
    for Book in Book_List:
        #print("ID: "+Book.split("/")[-1])
        Id = int(Book.split("/")[-1])

        '''
        for lang in g.triples((Book,DCTERM["language"],None)):
            print("Language: "+lang[2])
            for x in g.triples((lang,RDFN["value"],None)):
                print("X:"+x[2])
        '''     
        Publisher = ""
        for pub in g.triples((Book,DCTERM["publisher"],None)):
            #print("Publisher: "+pub[2])
            Publisher = str(pub[2].n3())
            Publisher = Publisher.decode('utf-8').encode('utf-8',"?")
        Rights=""
        for right in g.triples((Book,DCTERM["rights"],None)):
            #print("Rights: "+right[2])
            Rights = str(right[2].n3()).decode('utf-8').encode('utf-8','?')

        Title=""
        for title in g.triples((Book,DCTERM["title"],None)):
            #print("Title: "+title[2])
            try:
                Title = str(title[2].n3())
                Title = Title.decode('ascii').encode('ascii','?')
            except:
                Title = Title.decode('latin-1').encode('ascii','?')

        Download=""
        for download in g.triples((Book,PGTERMS["downloads"],None)):
            #print("Download: "+download[2])
            Download = str(download[2].n3()).decode('utf-8').encode('utf8','?')
        
        if "^^<http://www.w3.org/" in Download:
            Download = Download.split("^^")[0]

        format_List=[k for k in g.triples((Book,DCTERM["hasFormat"],None))]
        #print("File count: "+str(len(format_List)))

        epub_img=""
        epub_no_img=""
        kindle_img=""
        kindle_no_img=""
        zip_html=""
        text=""
        cover=""

        for format_ in format_List:
            data=str(format_[2].n3()).decode('utf-8').encode('utf8','?')
            if '<' in data:
                data=data[1:-1]
            if "kindle.images" in data:
                kindle_img = data
            elif "kindle.noimages" in data:
                kindle_no_img = data
            elif "epub.images" in data:
                epub_img = data
            elif "epub.noimages" in data:
                epub_no_img = data
            elif "-h.zip" in data:
                zip_html = data
            elif ".txt" in data:
                text = data
            elif "cover.medium.jpg" in data:
                cover = data

    return (Id,Title,Download,Rights,epub_img,epub_no_img,kindle_img,kindle_no_img,zip_html,text,cover)

def addToDB(books):
    mydb=mysql.connector.connect(user='root', password='root',
        host='127.0.0.1', database='gutenberg',
        auth_plugin='caching_sha2_password')

    mycursor = mydb.cursor()
    sql = "INSERT INTO ebooks (id,title,download,rights,epub_img,epub_no_img,kindle_img,kindle_no_img,zip_html,txt,cover) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    #val=[(1,'test','test','test','test','test','test','test','test','test','test')]
    mycursor.executemany(sql,books)
    mydb.commit()

    print(mycursor.rowcount, "was inserted.") 


limit = 57894
#print(rdfToTuple(File))

books=[]
count=0
for i in range(1600,limit+1):
    File="epub/"+str(i)+"/pg"+str(i)+".rdf"
    print(str(i) +" / " + str(limit))
    #print(rdfToTuple(File))
    if os.path.isfile(File):
        books.append(rdfToTuple(File))
        count+=1
        if count%200==0:
            print("Adding to DB...")
            addToDB(books)
            books=[]
            count=0

print("Checking block...")
if count>0:
    print("Some block left...\n inserting ...")
    addToDB(books)

print("Finished...........")