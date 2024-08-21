import pymongo
import mysql.connector
import streamlit as st
import pandas as pd
import plotly.express as px
import numpy as np

#task:1 Renaming the Column names
df = pd.read_excel("C:/Users/Sangeetha/Downloads/census_2011.xlsx")
df.rename(columns={"State name":"State_UT"}, inplace=True)
df.rename(columns={"District name":"District"}, inplace=True)
df.rename(columns={"Male_Literate":"Literate_Male"}, inplace=True)
df.rename(columns={"Female_Literate":"Literate_Female"}, inplace=True)
df.rename(columns={"Rural_Households":"Households_Rural"}, inplace=True)
df.rename(columns={"Urban_Households":"Households_Urban"}, inplace=True)
df.rename(columns={"Age_Group_0_29":"Young_and_Adult"}, inplace=True)
df.rename(columns={"Age_Group_30_49":"Middle_Aged"}, inplace=True)
df.rename(columns={"Age_Group_50":"Senior_Citizen"}, inplace=True)
df.rename(columns={"Age not stated":"Age_Not_Stated"}, inplace=True)
df.rename(columns={"Households_with_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car":"Households_with_TV_Computer_Lap_Telephone_mobile_and_Bike_Car"}, inplace=True)
df.rename(columns={"Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households":"Type_latrine_facility_Night_disposed_in_open_drain_Households"}, inplace=True)

#task:2 Rename State/UT Names
df.State_UT = df.State_UT.str.title()
df.replace("Jammu And Kashmir","Jammu and Kashmir",inplace=True)
df.replace("Andaman And Nicobar Islands","Andaman and Nicobar Islands",inplace=True)
print(df["State_UT"].tail())

#task:3 New State/UT formation
list1=["Adilabad","Nizamabad","Karimnagar","Medak","Hyderabad","Rangareddy","Mahbubnagar","Nalgonda","Warangal","Khammam"]
df.loc[df.District.isin(list1),'State_UT']='Telengana'
list2=["Leh(Ladakh)","Kargil"]
df.loc[df.District.isin(list2),'State_UT']='Ladakh'

#task:4 find and missing data
k=df.isnull().sum().sum()

df["Population"].fillna(df["Male"]+df["Female"],inplace=True)
df["Population"].fillna(df["Young_and_Adult"]+df["Middle_Aged"]+df["Senior_Citizen"]+df["Age_Not_Stated"],inplace=True)
df["Literate"].fillna(df["Literate_Male"]+df["Literate_Female"],inplace=True)
df["Households"].fillna(df["Households_Rural"]+df["Households_Urban"],inplace=True)
m=df.isnull().sum().sum()

df.rename(columns=lambda x: x[:62], inplace=True) #this will truncate the column name. Then print the dataframe

df.fillna(0,inplace=True)# changed null values to '0' because mysql couldn't read null values it deleting the complte row

#convertin py file to mongodb file

client=pymongo.MongoClient("mongodb://localhost:27017")
data=df.to_dict(orient="records")
db=client["Demo1"]
db.Censes.insert_many(data)

#connecting mongodb with mysql

connection=mysql.connector.connect(
    host="localhost",
    user="root",
    password="123456",
    database="proj1"
)
#print("connected")

cursor = connection.cursor()

print(cursor.column_names)
def main():
    st.title("PROJECT 1:Census Data Standardization and Analysis Pipeline")
    option=st.sidebar.selectbox("SELECT QN NO:",("1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20"))
    if option=="1":
        st.subheader("1:Total population of each district")
        cursor.execute("select State_UT,District,Population from censes_2011 order by State_UT")
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option == "2":
        st.subheader("2:How many literate males and females are there in each district")
        cursor.execute("select State_UT,District,Literate_Male,Literate_Female from censes_2011 order by State_UT")
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option == "3":
        st.subheader("3:Percentage of workers (both male and female) in each district")
        cursor.execute("select State_UT,District,concat((Workers/Population)*100,'%') as total_percent_of_literate from censes_2011 order by State_UT")
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option=="4":
        st.subheader("4:Households have access to LPG or PNG as a cooking fuel in each district")
        cursor.execute("select State_UT,District,LPG_or_PNG_Households from censes_2011 order by State_UT")
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option=="5":
        st.subheader("5:Religious composition (Hindus, Muslims, Christians, etc.) of each district")
        cursor.execute("""select State_UT,District,concat(Hindus/(Hindus+Muslims+Christians)*100,'%') as hindus_in_total_population,
        concat(Muslims/(Hindus+Muslims+Christians)*100,'%') muslims_in_total_population,
        concat(Christians/(Hindus+Muslims+Christians)*100,'%') christians_in_total_population from censes_2011 order by State_UT""")
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option=="6":
        st.subheader("4:Households have access to internet in each district")
        cursor.execute("select State_UT,District,Households_with_Internet from censes_2011 order by State_UT")
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option=="8":
        st.subheader("8:Households have access to various modes of transportation (bicycle, car, radio, scooter, etc.) in each district")
        cursor.execute("select State_UT,District,(Households_with_Bicycle + Households_with_Car_Jeep_Van + Households_with_Scooter_Motorcycle_Moped + Households_with_Radio_Transistor) as house_with_transportation from censes_2011 order by State_UT")
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option=="7":
        st.subheader("7:Educational attainment distribution (below primary, primary, middle, secondary, etc.) in each district")
        cursor.execute("select State_UT,District,concat((Below_Primary_Education*100)/Literate_Education,'%') as below_Primary_edu,concat((Primary_Education*100)/Literate_Education,'%') as Primary_edu,concat((Middle_Education*100)/Literate_Education,'%') as mid_edu,concat((Secondary_Education*100)/Literate_Education,'%') as sec_edu,concat((Higher_Education*100)/Literate_Education,'%') as high_edu from censes_2011 order by State_UT")
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option=="9":
        st.subheader("9:Condition of occupied census houses (dilapidated, with separate kitchen, with bathing facility, with latrine facility, etc.) in each district")
        cursor.execute("select State_UT,District,Condition_of_occupied_census_houses_Dilapidated_Households,Households_with_separate_kitchen_Cooking_inside_house,Having_bathing_facility_Total_Households,Having_latrine_facility_within_the_premises_Total_Households from censes_2011 order by State_UT")
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option=="10":
        st.subheader("10:Household size distributed (1 person, 2 persons, 3-5 persons, etc.) in each district")
        cursor.execute("""select Household_size_1_person_Households,Household_size_2_persons_Households,Household_size_1_to_2_persons,
        Household_size_3_persons_Households,Household_size_3_to_5_persons_Households,Household_size_4_persons_Households,
        Household_size_5_persons_Households,Household_size_6_8_persons_Households,Household_size_9_persons_and_above_Households from censes_2011 
        order by State_UT
        """)
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option=="11":
        st.subheader("11:Total number of households in each state")
        cursor.execute("select State_UT,sum(Households) as total_households from censes_2011 group by State_UT order by 2 desc")
        result1 = pd.DataFrame(cursor.fetchall())
        result1.columns = cursor.column_names
        fig = px.bar(result1,x='State_UT', y='total_households',text='total_households')
        st.write(fig)

    elif option=="12":
        st.subheader("12:Households have a latrine facility within the premises in each state")
        cursor.execute("select State_UT,sum(Having_latrine_facility_within_the_premises_Total_Households) as households_with_latriene from censes_2011 group by State_UT order by 2")
        result1 = pd.DataFrame(cursor.fetchall())
        result1.columns = cursor.column_names
        fig = px.bar(result1, x='State_UT', y='households_with_latriene',text='households_with_latriene')
        st.write(fig)


    elif option=="13":
        st.subheader("13:Average household size in each state")
        cursor.execute("select State_UT,avg(Households) as avg_households from censes_2011 group by State_UT")
        result1 = pd.DataFrame(cursor.fetchall())
        result1.columns=cursor.column_names
        fig = px.histogram(result1, x="State_UT", y="avg_households")
        st.write(fig)

    elif option =="14":
        st.subheader("14:Households are owned versus rented in each state")
        cursor.execute("select State_UT,sum(Ownership_Owned_Households) as own_house,sum(Ownership_Rented_Households) as rented_house from censes_2011 group by State_UT")
        result1 = pd.DataFrame(cursor.fetchall())
        result1.columns = cursor.column_names
        result1[["own_house", "rented_house"]] = result1[["own_house", "rented_house"]].apply(pd.to_numeric)
        fig = px.bar(result1, x="State_UT",y=["own_house","rented_house"])
        st.write(fig)
    elif(option == "15"):
        st.subheader("15:Distribution of different types of latrine facilities in each state")
        cursor.execute("select State_UT,sum(Type_of_latrine_facility_Pit_latrine_Households) as pit_latriene,sum(Type_of_latrine_facility_Other_latrine_Households) as other_latreiene,sum(Type_latrine_facility_Night_disposed_in_open_drain_Households) as night_soil_latriene,sum(Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to) as flush_latriene from censes_2011 group by State_UT")
        result1 = pd.DataFrame(cursor.fetchall())
        result1.columns = cursor.column_names
        result1[["pit_latriene", "other_latreiene","night_soil_latriene","flush_latriene"]] = result1[["pit_latriene", "other_latreiene","night_soil_latriene","flush_latriene"]].apply(pd.to_numeric)
        fig = px.bar(result1, x="State_UT", y=["pit_latriene", "other_latreiene","night_soil_latriene","flush_latriene"])
        st.write(fig)
    elif option =="16":
        st.subheader("16:Households have access to drinking water sources near the premises in each state")
        cursor.execute("select State_UT,sum(Location_of_drinking_water_source_Near_the_premises_Households) as sources_near from censes_2011 group by State_UT")
        result1 = pd.DataFrame(cursor.fetchall())
        result1.columns=cursor.column_names
        fig=px.pie(result1,values='sources_near',names='State_UT')
        st.write(fig)
    elif option=="17":
        st.subheader("17:Average household income distribution in each state based on the power parity categories")
        cursor.execute("select State_UT,avg(Power_Parity_Less_than_Rs_45000) as pp_1,avg(Power_Parity_Rs_45000_90000) as pp_2,avg(Power_Parity_Rs_90000_150000) as pp_3,avg(Power_Parity_Rs_45000_150000) as pp_4,avg(Power_Parity_Rs_150000_240000) as pp5,avg(Power_Parity_Rs_240000_330000) as pp6,avg(Power_Parity_Rs_150000_330000) as pp7,avg(Power_Parity_Rs_330000_425000) as pp8,avg(Power_Parity_Rs_425000_545000) as pp9 from censes_2011 group by State_UT ")
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option=="18":
        st.subheader("18:The percentage of married couples with different household sizes in each state")
        cursor.execute("""with cte as
                            (
                            select State_UT,sum(Married_couples_1_Households+Married_couples_2_Households+Married_couples_3_Households+Married_couples_3_or_more_Households+Married_couples_4_Households+Married_couples_5__Households+Married_couples_None_Households) as s 
                            from censes_2011 group by State_UT
                            ) 
        select c.State_UT,
        concat((sum(c.Married_couples_1_Households)/cte.s)*100,'%') as married_couples_1,
        concat((sum(c.Married_couples_2_Households)/cte.s)*100,'%') as married_couples_2,
        concat((sum(c.Married_couples_3_Households)/cte.s)*100,'%') as married_couples_3,
        concat((sum(c.Married_couples_3_or_more_Households)/cte.s)*100,'%') as married_couples_4,
        concat((sum(c.Married_couples_4_Households)/cte.s)*100,'%') as married_couples_5,
        concat((sum(c.Married_couples_5__Households)/cte.s)*100,'%') as married_couples_6,
        concat((sum(c.Married_couples_None_Households)/cte.s)*100,'%') as married_couples_7 
        from censes_2011 c join cte on c.State_UT=cte.State_UT group by c.State_UT"""
                       )
        test = pd.DataFrame(cursor.fetchall())
        test.columns = cursor.column_names
        st.write(test)
    elif option == "19":
        st.subheader("19:Households fall below the poverty line in each state based on the power parity categories")
        cursor.execute("select State_UT,sum(Power_Parity_Less_than_Rs_45000) as poverty_line from censes_2011 group by State_UT")
        result1 = pd.DataFrame(cursor.fetchall())
        result1.columns = cursor.column_names
        fig = px.pie(result1, values='poverty_line', names='State_UT')
        st.write(fig)

    elif option=="20":
        st.subheader("20:overall literacy rate (percentage of literate population) in each state")
        cursor.execute("select State_UT,concat((sum(Literate)/(select sum(Literate) as s from censes_2011))*100,'%') as percentage_of_literate from censes_2011 group by State_UT order by 2 desc")
        result1 = pd.DataFrame(cursor.fetchall())
        result1.columns = cursor.column_names
        fig = px.bar(result1, x="State_UT", y="percentage_of_literate")
        st.write(fig)




if __name__=="__main__":
    main()