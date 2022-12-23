#etl.py
#import Library
import glob
import json
import os
from typing import List

import psycopg2


table_insert_customer    = "INSERT INTO customer VALUES %s ON CONFLICT DO NOTHING;"
table_insert_garage     = "INSERT INTO garage VALUES %s ON CONFLICT DO NOTHING;"
table_insert_cailm   = "INSERT INTO cailm VALUES %s ON CONFLICT DO NOTHING;"




def get_files(filepath: str) -> List[str]:
    """
    Description: This function is responsible for listing the files in a directory
    """

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.csv"))
        # files = glob.glob(os.path.join(root, "github_events.json"))
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
                sql_insert = ''

                # Insert for customer
                val = each["customer"]["Cus_Id"], each["customer"]["FullName"], 
                each["customer"]["EmailAddress"],  each["customer"]["Requested_Amount"],each["customer"]["Age"],
                each["customer"]["Requested_Amount"].each["customer"]["Applicant_State_Desc"],
                each["customer"]["AEmployment_Type_Desc11"],each["customer"]["AEmployment_Type_Desc11"],
                each["customer"]["Applicant_City_Desc "].each["customer"]["Total_Work_Experience"] ,
                each["customer"]["ProducName1"].each["Cost of Vehicle"]["Total_Work_Experience"] 
                each["customer"]["ProducName1"].each["customer"]["Total_Work_Experience"] 
 
                sql_insert = table_insert_customer % str(val)
                cur.execute(sql_insert)

                # Insert for garage
                val = each["garage"]["Garage_Id"], each["Garage"]["name"],each["Garage"],["phoneNumber"],each["Garage"],["Address"]  
                sql_insert = table_insert_Garage % str(val)
                cur.execute(sql_insert)

                # Insert for cailm
                try:
                    val = each["cailm"]["cailm_id"], each["cailm"]["cailm_Register"], each["cailm"]["cailm_Type"], each["cailm"]["cailm_level"],each["cailm"]["cailm_Comment"],each["cailm"]["cailm_cost"],each["cailm"]["staft_ID"],each["cailm"]["GarageId"],each["cailm"]["CusID"]
                    sql_insert = table_insert_cailm % str(val)
                    cur.execute(sql_insert)
                except: pass

            
            conn.commit()


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=postgres password=postgres"
    )
    cur = conn.cursor()

    process(cur, conn, filepath="../data/")

    conn.close()


if __name__ == "__main__":
    main()
