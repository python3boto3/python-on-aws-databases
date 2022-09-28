# requires pip install python-dotenv, pip install paramiko as python layer

import os
from dotenv import load_dotenv
import paramiko
import pandas as pd
import datetime

def lambda_handler(event, context):

    # Read from .env file.
    load_dotenv()
    env = os.environ.get

    # Create SSH client.
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Establish SFTP connection to origin remote server.
    ssh.connect(
        hostname=env("hostName"),
        username=env("userName"),
        password=env("oPw"),
        port=22
    )

    print("SSH connection complete.")
    sftp_client = ssh.open_sftp()
    print("SFTP connection complete.")

    # Download "input_file.csv" from origin remote server.
    sftp_client.get(env("ORIGIN_FILE_PATH"), "/tmp/src.csv")
    print("Successfully downloaded 'src.csv'.")

    # Close SFTP connection to origin remote server.
    sftp_client.close()
    print("SFTP connection ended.")
    ssh.close()
    print("SSH connection ended.")

    # Create Pandas dataframe with downloaded file.
    df = pd.read_excel(r"/tmp/src.csv")

    # Create df that will be the first output file.
    df1 = df

    # Create function that takes two columns (date & time)
    # and creates a new single date_time column.

    def get_datetime(date, time):
        date_time = date.strftime("%Y-%m-%d") + " " + str(time)
        return date_time

    # Apply above function.
    df1["sampleRxDtTm"] = df1.apply(
        lambda row: get_datetime(
            row["Received Date\n(YYYY-MM-DD)"],
            row["Received Time\n(24-hour format)"],
        ),
        axis=1)

    # Add "OrdersID" column, rename some columns,
    # and select the desired columns for output.

    df1["OrdersID"] = ""
    df1 = df1.rename(
        columns={"Accession No. or\nClient Sample ID": "aNumber"}
    )
    df1 = df1[[
        "ordersID",
        "sampleRxDtTm",
        "aNumber",
    ]]

    # Write df1 to "rFile.csv".
    df1.to_csv(r"/tmp/rFile.csv")
    print("Successfully written df1 to 'rFile.csv'.")

    # Create df that will be the second output file.
    df2 = df

    # Add "OrdersID" column, rename some columns,
    # and select the desired columns for output.
    df2["OrdersID"] = ""
    df2 = df2.rename(columns={
        "Received Date\n(YYYY-MM-DD)": "rcptDt",
        "Reported Date\n(YYYY-MM-DD)": "returnDt",
        "Result\n(Detected / Undetected)": "Result"
    })
    df2 = df2[[
        "OrdersID",
        "Result",
        "rcptDt",
        "returnDt",
    ]]

    # Write df2 to "Resp.csv".
    df2.to_csv(r"/tmp/Resp.csv")
    print("Successfully written df2 to 'Resp.csv'.")

    # Establish SFTP connection to destination remote server.
    ssh.connect(
        hostname=env("dHost"),
        username=env("dUser"),
        password=env("dPw"),
        port=22
    )

    print("SSH connection complete.")
    sftp_client = ssh.open_sftp()
    print("SFTP connection complete.")

    # Upload "rFile.csv" and "Resp.csv" to
    # destination remote server.
    sftp_client.put(
        "/tmp/rFile.csv",
        env("dPath1"))
    sftp_client.put(
        "/tmp/Resp.csv",
        env("dPath2"))
    print("Successfully uploaded 'rFile.csv' \
        and 'Resp.csv' to destination remote server.")

    # Close SFTP connection to destination remote server.
    sftp_client.close()
    print("SFTP connection ended.")
    ssh.close()
    print("SSH connection ended.")

    print('\nOperation complete.')
