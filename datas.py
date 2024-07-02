import pandas as pd
import mysql.connector
mydb=mysql.connector.connect(
host="localhost",
user="root",
password="12345",
database="project2_db"
)

def csv_file_view():
    view_data = pd.read_csv('boxoffice data.csv')
    # print(view_data)
    return view_data

def kalki_filter():
    view_data=csv_file_view()
    kalki_movie=view_data[view_data['MOVIE NAME'].str.lower() == 'kalki']
    print(kalki_movie)
    return kalki_movie

# mysql to python connection line
mycursor=mydb.cursor()
# INSERT INTO KALKI TABLE(KALKI MOVIE DATAS)
kalki_movie=kalki_filter()
for index, row in kalki_movie.iterrows():
    mycursor.execute("insert into kalki (CUSTOMER_NAME,MOVIE_NAME,SHOWS,TICKET,TICKET_COST,BILL_WITH_GST) values (%s,%s,%s,%s,%s,%s)",(row['CUSTOMER NAME'], row['MOVIE NAME'], row['SHOWS'], row['TICKET'], row['TICKET_COST'], row['BILL_WITH_GST']))

    mydb.commit()

def maharaja_filter():
    view_data=csv_file_view()
    maharaja_movie=view_data[view_data['MOVIE NAME'].str.lower() == 'maharaja']
    print(maharaja_movie)
    return maharaja_movie

# INSERT INTO MAHARAJA TABLE(MAHARAJA MOVIE DATAS)
maharaja_movie=maharaja_filter()
for index, row in maharaja_movie.iterrows():
    mycursor.execute("insert into maharaja (CUSTOMER_NAME,MOVIE_NAME,SHOWS,TICKET,TICKET_COST,BILL_WITH_GST) values (%s,%s,%s,%s,%s,%s)",(row['CUSTOMER NAME'], row['MOVIE NAME'], row['SHOWS'], row['TICKET'], row['TICKET_COST'], row['BILL_WITH_GST']))

mydb.commit()

def garudan_filter():
    view_data=csv_file_view()
    garudan_movie=view_data[view_data['MOVIE NAME'].str.lower() == 'garudan']
    print(garudan_movie)
    return garudan_movie

# INSERT INTO GARUDAN TABLE(GARUDAN MOVIE DATAS)
garudan_movie=garudan_filter()
for index, row in garudan_movie.iterrows():
    mycursor.execute("insert into garudan (CUSTOMER_NAME,MOVIE_NAME,SHOWS,TICKET,TICKET_COST,BILL_WITH_GST) values (%s,%s,%s,%s,%s,%s)",(row['CUSTOMER NAME'], row['MOVIE NAME'], row['SHOWS'], row['TICKET'], row['TICKET_COST'], row['BILL_WITH_GST']))

mydb.commit() 

def ptsir_filter():
    view_data=csv_file_view()
    pt_sir_movie=view_data[view_data['MOVIE NAME'].str.lower() == 'pt sir']
    print(pt_sir_movie)
    return pt_sir_movie

# INSERT INTO PT_SIR TABLE(PT SIR MOVIE DATAS)
pt_sir_movie=ptsir_filter()
for index, row in pt_sir_movie.iterrows():
    mycursor.execute("insert into pt_sir (CUSTOMER_NAME,MOVIE_NAME,SHOWS,TICKET,TICKET_COST,BILL_WITH_GST) values (%s,%s,%s,%s,%s,%s)",(row['CUSTOMER NAME'], row['MOVIE NAME'], row['SHOWS'], row['TICKET'], row['TICKET_COST'], row['BILL_WITH_GST']))

mydb.commit()

csv_file_view()
kalki_filter()
maharaja_filter()
garudan_filter()
ptsir_filter()




    
