# swu-ds525
Capstone Project -Sarwan Puteh

ขั้นตอนการทำมีดังนี้
1. เปลี่ยน Working Directory ไปยัง capstone-projectc :
    $ cd capstone-project
 -สร้าง Python virtual environment สำหรับการทำงาน (ครั้งแรก):
    $python -m venv ENV
  -Activate เพื่อเข้าสู่ Virtual environment:
    $ source ENV/bin/activate
 -ทำการติดตั้ง package ที่จำเป็นในการทำงาน (ครั้งแรก):
    $ pip install -r prerequisite/requirements.txt

2. จัดเตรียมการเชื่อมต่อ Cloud Environment (AWS):
$ cat ~/.aws/credentials
  นำ 3 ค่าด้านล่างไป update ที่ source code สำหรับการเชื่อมต่อ AWS
  aws_access_key_id
  aws_secret_access_key
  aws_session_token

 -source code ที่ต้องการการ update ในไฟล์
   datalake_s3.ipynb
   
  3. จัดเตรียมพื้นที่จัดเก็บบน AWS S3:
  สร้าง S3 bucket พร้อมเปิด Public Access
  เนื่องจากมีคอลัมน์หรือโครงสร้างที่ต่างกันจึงต้องจัดเก็บ raw data ดังนี้
  -  uncleancustomer/ (เก็บ raw data)
  -  uncleancailm/ (เก็บ raw data)
  -  uncleangarage/ (เก็บ raw data)
  -  uncleancars/(เก็บ raw data)
  -  uncleanstaft/ (เก็บ raw data)
  -  uncleaninsurance/(เก็บ raw data)
    ท้ายที่สุดแล้วเราจะเก็บ data หลังจากการ Transform ข้อมูล raw data ไปเป็น Cleaned data ในfolder exportdata Cleaned data จะถูกจัด       เก็บบน S3 โดยมีการทำ partition
    
   จัดเตรียม Docker สำหรับการ run project :
   -จากนั้นให้รันคำสั่งด้านล่างเพื่อ start Docke
   $ docker-compose up
   
   
   4.datalake 
     -Transform ข้อมูล raw data ไปเป็น Cleaned data:
     -เข้าสู่ PySpark Notebook UI ด้วย port 8888 (localhost:8888)
     -Run PySpark Notebook "datalake_s3.ipynb"
     -Cleaned data จะถูกจัดเก็บบน S3 โดยมีการทำ partition
    
   5.datawarehouse ในที่นี้ใช้ postgres  เนื่องจาก redshift ใช้ไม่ได้เนื่องจาก Account aws ผมใช้ redshift ไม่ได้จึงใช้ postres ในการ           tranform data ทำdata และ export  dataที่พร้อมใช้งาเพื่อจะไปทำ data visualization 
   
   6.สร้าง Dashboard ด้วย Power BI:
    สร้าง Dashboard เพื่อนำเสนอข้อมูลเพื่อตอบคำถามหรือแก้ไขปัญหา
    คลิกลิงค์เพื่อดูในtableau
  https://public.tableau.com/app/profile/sarwan.puteh/viz/Book1_16716020019610/Dashboard1?publish=yes

![21122022](https://user-images.githubusercontent.com/12684425/208823219-b41eea9d-b158-4fb7-9334-c87eb8183925.PNG)
