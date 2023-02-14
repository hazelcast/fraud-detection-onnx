import streamlit as st
import pandas as pd
import plotly.figure_factory as ff
import plotly.graph_objects as go
import hazelcast
import plotly.express as px
import os
from streamlit_plotly_events import plotly_events

st.set_page_config(layout="wide")

@st.cache_resource
def get_hazelcast_client(cluster_members=['127.0.0.1']):
    client = hazelcast.HazelcastClient(**{'cluster_members':cluster_members})
    #run Mapping required to run SQL Queries on JSON objects in predictionResult Map
    client.sql.execute(
                """
            CREATE OR REPLACE MAPPING predictionResult (
            __key VARCHAR,
            transaction_number VARCHAR,
            transaction_date VARCHAR,
            amount DECIMAL,
            merchant VARCHAR,
            merchant_lat DOUBLE,
            merchant_lon DOUBLE,
            credit_card_number BIGINT,
            customer_name VARCHAR,
            customer_city VARCHAR,
            customer_age_group VARCHAR,
            customer_gender VARCHAR,
            customer_lat DOUBLE,
            customer_lon DOUBLE,
            distance_from_home REAL,
            transaction_weekday_code INT,
            transaction_hour_code INT,
            transaction_month_code INT,
            fraud_model_prediction INT,
            fraud_probability DOUBLE,
            inference_time_ns BIGINT,
            transaction_processing_total_time BIGINT
            )
            TYPE IMap
            OPTIONS (
                'keyFormat' = 'varchar',
                'valueFormat' = 'json-flat');
                """
            ).result()
    return client

@st.cache_data
def get_df(_client, sql_statement, date_cols):
    #mapping hazelcast SQL Types to Pandas dtypes
    sql_to_df_types = {0:'category',6:'float32',8:'float32',5:'int32',7:'float32',4:'int32',1:'bool'}

    sql_result = client.sql.execute(sql_statement).result()

    #get column metadata from SQL result
    metadata = sql_result.get_row_metadata()
    column_names = [c.name for c in metadata.columns]
    column_types = [sql_to_df_types[c.type] for c in metadata.columns]
    columns_dict = dict(zip(column_names, column_types))


    #build a dict col_name -> list of values 
    column_values = {}
    for c in column_names:
        column_values[c] = []
    for row in sql_result:
        for c in column_names:
            value = row.get_object(c)
            column_values[c].append(value)
            
    #create dataframe
    df = pd.DataFrame({key: pd.Series(values) for (key, values) in column_values.items()})
    #apply the right data type of each column
    df = df.astype(columns_dict)
    
    #additional cols for every datetime col
    for col in date_cols:
        if col in column_names:
            df[col] = pd.to_datetime(df[col])
            df[col+'_day_of_week'] = df[col].dt.dayofweek
            df[col+'_month'] = pd.DatetimeIndex(df[col]).month
            df[col+'_hour'] = df[col].dt.hour
    
    return df

@st.cache_data
def get_dashboard_totals(fraud_probability_threshold):
    result = {}
    sql_statement = 'SELECT count(*) as total_records, sum(amount) as total_amount, avg(amount) as avg_amount, avg(distance_from_home) as avg_distance_km FROM predictionResult LIMIT 1'
    sql_result = client.sql.execute(sql_statement).result()
    for row in sql_result:
        result['total_records'] = row.get_object('total_records')
        result['total_amount'] = round(float(row.get_object('total_amount')),2)
        result['avg_amount'] = round(float(row.get_object('avg_amount')),2)
        result['avg_distance_km'] = round(float(row.get_object('avg_distance_km')),2)

    sql_statement = '''
            SELECT count(*) as potential_fraud_records,
            sum(amount) as potential_fraud_amount, 
            avg(amount) as potential_fraud_per_transaction, 
            avg(distance_from_home) as avg_distance_in_potential_fraud_transaction
            FROM predictionResult 
            WHERE fraud_probability > ? 
            LIMIT 1
        '''
    sql_result = client.sql.execute(sql_statement,(fraud_probability_threshold)).result()
    for row in sql_result:
        result['potential_fraud_records'] = row.get_object('potential_fraud_records')
        result['potential_fraud_amount'] = round(float(row.get_object('potential_fraud_amount')),2)
        result['potential_fraud_per_transaction'] = round(float(row.get_object('potential_fraud_per_transaction')),2)
        result['avg_distance_in_potential_fraud_transaction'] = round(float(row.get_object('avg_distance_in_potential_fraud_transaction')),2)
    
    return result

@st.cache_data
def get_categorical_variables():
    categorical_features =['customer_name','customer_city','customer_age_group','customer_gender']
    return categorical_features

#Connect to hazelcast - use env variable HZ_ONNX, if provided
hazelcast_node = os.environ['HZ_ONNX']
if hazelcast_node:
    client = get_hazelcast_client([hazelcast_node])
else:
    client = get_hazelcast_client()

#get categorical variable names
categorical_features = get_categorical_variables()


#sidebar 
st.sidebar.header('Fraud Probability Threshold')
probability_threshold = st.sidebar.slider('Enter Threshold',0,100,70,1)
st.sidebar.header('Key Dimensions','key dimensions')
category_selected = st.sidebar.selectbox('', categorical_features)


#Continue Loading data
fraud_threshold = probability_threshold / 100
totals = get_dashboard_totals(fraud_threshold)

#Main page title and header
st.title('Fraud Analysis Dashboard')
st.header('All Trasactions','tx_metrics')
col1, col2, col3,col4  = st.columns(4)
with col1:
    st.metric('Total Transactions', totals['total_records'],help='SELECT count(*) from predictionResult')
with col2:
    st.metric('Total Amount', totals['total_amount'],help='Total Trasaction Amount')
with col3:
    st.metric('Avg Amount', totals['avg_amount'],help='Avg Transaction Amount')
with col4:
    st.metric('Avg Distance (km) from home',totals['avg_distance_km'],help='Distance from home in Km')

#Suspected Fraud Summary
st.header('Suspected Fraudulent Transactions','tx_fraud_metrics')
col1_f, col2_f, col3_f,col4_f  = st.columns(4)
with col1_f:    
    st.metric('Total Transactions',totals['potential_fraud_records'],help='SELECT count(*) from predictionResult where fraud_probability > ' + str(probability_threshold) + '%')
with col2_f:
    st.metric('Total Amount ', totals['potential_fraud_amount'],help='Total $ Amount of Predicted Fraud in Transactions with Fraud probability > ' + str(probability_threshold) + '%')
with col3_f: 
    avg_amount_delta = round(totals['potential_fraud_per_transaction'] - totals['avg_amount'],2)
    st.metric('Avg Amount per Transaction',  totals['potential_fraud_per_transaction'], avg_amount_delta,help='Avg Transaction Amount in Transactions with Fraud probability > ' + str(probability_threshold) + '%')
with col4_f:
    avg_distance_delta = round(totals['avg_distance_in_potential_fraud_transaction'] - totals['avg_distance_km'],2)
    st.metric('Avg Distance (km) from home', totals['avg_distance_in_potential_fraud_transaction'],avg_distance_delta,help='Distance from home in Km in Transactions with Fraud probability > ' + str(probability_threshold) + '%')

#Fraud Sources Header
st.header('Where is Potential Fraud Coming from?','key_dimensions')
col_chart1, col_chart2 = st.columns(2)

#Bubble Chart
sql_statement = """
    select count(*) as total_transactions, avg(fraud_probability) as fraud_probability,
        avg(distance_from_home) as distance_from_home,sum(amount) as total_amount,
        {category_selected} 
    from predictionResult 
    where fraud_probability > {fraud_threshold} 
    group by {category_selected}
    order by fraud_probability DESC
    limit 100
    """.format(category_selected=category_selected,fraud_threshold=fraud_threshold)


df_bubble = get_df(client,sql_statement=sql_statement,date_cols=[])

fig = px.scatter(df_bubble, x="total_amount", y="total_transactions",
	         size="total_amount", color=category_selected,
                 hover_name="fraud_probability", log_x=True, size_max = 40, width=500, height=500)
with col_chart1:
    st.subheader("Fraud Hotspots by " + category_selected )
    #st.plotly_chart(fig)
    selected_points = plotly_events(fig,click_event=True,hover_event=False)
    

#MAP chart  - #Start by getting impacted from bubble chart
categorical_impacted = df_bubble[category_selected].to_list()
categorical_values= '\'' + '\',\''.join(categorical_impacted) + '\''

# if user already chosen a bubble on chart
total_amount_chosen = 0
if (selected_points):
    total_amount_chosen = selected_points[0]['x']
    selected_value = df_bubble.loc[df_bubble['total_amount'] == total_amount_chosen ][category_selected].to_list()[0]
    categorical_values = '\'' + selected_value + '\''

#query for merchant lat/lon - filter to only most impacted values displayed in bubble chart
sql_statement = """
    select merchant_lat,merchant_lon
    from predictionResult 
    where fraud_probability > {fraud_threshold} and {category_selected} in ({categorical_values})
    limit 1000
    """.format(category_selected=category_selected,categorical_values=categorical_values,fraud_threshold=fraud_threshold)

df_map = get_df(client,sql_statement=sql_statement,date_cols=[])
df_map['lat'] = df_map['merchant_lat']
df_map['lon'] = df_map['merchant_lon']
df_map = df_map.drop(['merchant_lat','merchant_lon'],axis=1)

with col_chart2:
    st.subheader("Merchant Locations")
    st.map(df_map)
    
#Analyst SQL Playground
st.header('Analyst - SQL Playground','sql_playground')
sql_statement = st.text_area('Enter a SQL Query', 'SELECT * \nFROM predictionResult \nLIMIT 100',200)

deploy_heuristic_button = st.button('Deploy New Fraud Detection Pattern', type="secondary", disabled=False)
#st.write(deploy_heuristic_button)
if deploy_heuristic_button:
    st.balloons()

#SQL Results
st.header('SQL Results','data')
if sql_statement:
    df3 = get_df(client,sql_statement,['transaction_date'])
    st.write(df3)