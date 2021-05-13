import re
import os
basepath = os.getcwd()
input_path = r"C:\Users\akuma981\DDL_AUTOMATION\input\table_DDL.sql" 
output_path = r"C:\Users\akuma981\DDL_AUTOMATION\output_json"
Template_path= r"C:\Users\akuma981\DDL_AUTOMATION\output_template\template.json"
dataset_id="google_bigquery_dataset.bigqry_fidw.dataset_id"

inputfile= open(input_path, 'r')

Lines = inputfile.readlines()
nooffiles=0

regexpat = re.compile(r'create table[\s\S]+?;')
x=regexpat.findall(str(Lines))
total_number_table=len(x)
cnt=0
num=0
if os.path.exists(Template_path):
  os.remove(Template_path)

for i in x:
    
    #print("i",i) i has entire DDl statement
    a=(i.split('\\n'))
    
    # primary key identification loop
    
#    p=''
#    for j in a:5
#        line = (re.sub("', '",'',j))
#        if 'primary' in line:
#            p=line.strip().split("(")[-1][:-1]
#            #print(p)
    dtype=['DOUBLE','lvarchar','varchar','char','SMALLFLOAT','smallint','integer','FLOAT','decimal','date','datetime','serial','Interval']
    cnt=0
    num=0
    for j in a:
        
       
    
       for x in dtype:
           if('create' not in j):
               if x in j:
                   #print(cnt,j)
                   cnt=cnt+1
                   break
    
    for j in a:
        
        #print(j)
        line = (re.sub("', '",'',j))
        #print(line)
        y=''
        #print(line)
        if 'create table' in line:
            #print(line)
            nooffiles=nooffiles+1
            #print("line",line)
            # line has sample value create table "i0572099".te_060131kro 
            filename = line.split(' ')[2]
            
            
            filename = re.sub('"','',filename)
            #print('filename',filename.split(".")[1])
            #filename=filename.replace(".","_")
            filename=filename.split(".")[1]
            #print("filename",filename)
            
            ## Template creation
            Template_file = open(Template_path, 'a')
            Template_file.writelines("\n")
            Template_file.writelines('#new table')
            Template_file.writelines("\n")
            Template_file.writelines('data "local_file" "{}" '.format(filename)+'{')
            Template_file.writelines("\n")
            Template_file.writelines('filename = "./inputs/{}.json"'.format(filename))
            Template_file.writelines("\n")
            Template_file.writelines("}")
            Template_file.writelines("\n")
            Template_file.writelines('resource "google_bigquery_table" "tbl-{}"'.format(filename))
            Template_file.writelines("{")
            Template_file.writelines("\n")
            Template_file.writelines('dataset_id = {}'.format(dataset_id))
            Template_file.writelines("\n")
            Template_file.writelines('table_id  = "{}"'.format(filename))
            Template_file.writelines("\n")
            Template_file.writelines('deletion_protection = false')
            Template_file.writelines("\n")
            Template_file.writelines('time_partitioning {')
            Template_file.writelines("\n")
            Template_file.writelines('type = "DAY"')
            Template_file.writelines("\n")
            Template_file.writelines('}')
            Template_file.writelines("\n")
            Template_file.writelines('labels = {')
            Template_file.writelines("\n")
            Template_file.writelines('env = "default"')
            Template_file.writelines("\n")
            Template_file.writelines("}")
            Template_file.writelines("\n")
            Template_file.writelines('schema = data.local_file.{}.content'.format(filename))
            Template_file.writelines("\n")
            Template_file.writelines('}')
            Template_file.writelines("\n")
            
            Template_file.close()


            
                                
                                
            
            
            ######
            print('Processing file ',nooffiles,'/',total_number_table, '--> ',filename)
            path = os.path.join(output_path, filename+'.json')
            #print("path",path)
            outputfile = open(path, 'w')
            outputfile.writelines('[')
            outputfile.writelines("\n")
            #cnt=1


            #outputfile.writelines(x)
            
        
        elif 'DOUBLE' in line:
            
            y=re.sub(r'DOUBLE\(.*\)', "Numeric", line)
            #print(line,x)
        elif 'lvarchar' in line:
            y=re.sub(r'lvarchar', "STRING", line)

        
        elif 'varchar' in line:
            y=re.sub(r'varchar\(.*\)', "STRING", line)
            #print("varchar",y,line)
        elif 'char' in line:
            y=re.sub(r'char\(.*\)', "STRING", line)
        elif 'SMALLFLOAT' in line:
            y=re.sub(r'SMALLFLOAT', "NUMERIC", line)

        elif 'smallint' in line:
            y=re.sub(r'smallint', "INTEGER", line)
        elif 'integer' in line:
            y=re.sub(r'integer', "INTEGER", line)

        elif 'FLOAT' in line:
            y=re.sub(r'FLOAT', "NUMERIC", line)
        elif 'decimal' in line:
            y=re.sub(r'decimal', "NUMERIC", line)
        elif 'date' in line:
            y=re.sub(r'date', "DATE", line)
        elif 'DATETIME' in line:
            y=re.sub(r'DATETIME', "DATETIME", line)
        elif 'INTERVAL' in line:
            y=re.sub(r'INTERVAL', "STRING", line)
        elif 'serial' in line:
            y=re.sub(r'serial', "INTEGER", line)
        
        elif 'default' in line:
            y=''
        elif 'unique' in line:
            y=''

            
        else:
            continue
              
        if 'not null' in line:
            col_constraint=',"mode": "REQUIRED"'
        else:
            col_constraint=',"mode": "NULLABLE"'
            


        #col_constraint=',"mode": "REQUIRED"'
        #print(str(x).split(' ')[-1])
        # name strinbg
        
        #print(x,'hi')
        if(y!=''):
            num=num+1
            #print(cnt,num)
            temp = re.search(r'[a-z]', y, re.I)
            #y=" ".join(re.split("[^a-zA-Z]*", y))
            y=y[temp.start():]
            
            col_name=str(y.strip()).split(' ')[0]
            col_type = str(y.strip()).split(' ')[1]
            col_type = " ".join(re.split("[^a-zA-Z]*", col_type))

            #print(num,cnt)
            if(num!=cnt):
                value='{"name": "'+str(col_name)+'","type": "'+str(col_type).strip()+'"'+ col_constraint+'},'
            else:
                value='{"name": "'+str(col_name)+'","type": "'+str(col_type).strip()+'"'+ col_constraint+'}'
           
                
            #print(value,line)
            outputfile.writelines(value)
            outputfile.writelines("\n")
    

    outputfile.writelines("\n")
    outputfile.writelines(']')
    outputfile.close()
    