import glob
import json
import os
from typing import List

import psycopg2


def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.csv"))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    return all_files


def process(cur, conn, filepath):
    # Get list of files from filepath
    all_files = get_files(filepath)

    for datafile in all_files:
        with open(datafile, "r") as f:
            data = json.loads(f.read())
            for each in data:
              
                
               
                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO customer (
                         Cus_ID int
                        ,FullName txt
                        ,EmailAddress txt
                        ,Requested_Amount int
                        ,Age int
                        ,Applicant_State_Desc As State txt
                        ,Applicant_City_Desc As City txt
                        ,Employment_Type_Desc11 txt
                        ,Total_Work_Experience int
                        ,Product_Name1 txt
                        ,Loan_Term int
                        ,Cost_Of_Vehicle int
                        ,Insurance_Code  int
                    ) 
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                # Insert data into tables here
                insert_statement = f"""
                    INSERT INTO events (
                         GarageID int
                        ,name txt
                        ,PhoneNumber txt
                        ,Address  txt
                    ) VALUES ('{each["GarageID"]}', '{each["name"]}','{each["phoneNumber"]}'. {each["Address"]}')
                    ON CONFLICT (id) DO NOTHING
                """
                # print(insert_statement)
                cur.execute(insert_statement)

                conn.commit()


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="/workspace/swu-ds525/Capstone_FinalProject/data")

    conn.close()


if __name__ == "__main__":
    main()
