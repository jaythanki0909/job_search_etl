from apify_client import ApifyClient
from chalice import Chalice
import pandas as pd
from datetime import timezone,timedelta
from dateutil.relativedelta import relativedelta 
import datetime 
import os
from openai import OpenAI
from opensearchpy import OpenSearch, helpers

app = Chalice(app_name='job')
app.debug = True
openai_client = os.environ['OPENAI_KEY']

def extractJobDate(data):
    return data.get('postedAt')

def doc_generator(index_name,df):
        for i, row in df.iterrows():
            doc = {
                "_index": index_name,
                "_source": row.to_dict(),
            }
            yield doc
def dateFormat(data):
    dt = datetime.datetime.now(timezone.utc) 
    # print(type(dt))
    if data is not None:
        count = data.split()[0]
        ago = data.split()[1]
        if ago.startswith("hour"):
            return dt - timedelta(hours=int(count))
        elif ago.startswith("day"):
            return dt - timedelta(days=int(count))
        elif ago.startswith("month"):
            return dt - relativedelta(months=int(count))
    else:
        return dt

def opneAiSummary(data):
    prompt = f"""
        Given the following job description and highlights, summarize the key information, return in point form:
        - job title:
        - company:
        - key skills required:
        - year of experience required:
        - location:
        - salary range:
        Job Description: {data['description']}
        Job Highlights: {data['jobHighlights']}
        """
    response = openai_client.chat.completions.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "user", "content": prompt}
        ])

    summary = response.choices[0].message.content.strip()
    return summary  

@app.schedule('cron(0/30 * * * ? *)')
# @app.route('/')
def job_ETL(event):
    
    client = ApifyClient("")
    job_titles = ["Data Analyst", "Data Engineer", "Data Scientist", "Software Developer", "Product Manager"]
    locations = {"w+CAIQICIHVG9yb250bw==": "Toronto", "w+CAIQICIJVmFuY291dmVy": "Vancourver"} 
    df = pd.DataFrame()
    for job in job_titles:
        for location in locations :

            run_input = {
                "startUrls": ["https://www.google.com/search?sca_esv=593729410&q=Software+Engineer+jobs&uds=AMIYvT8-5jbJIP1-CbwNj1OVjAm_ezkS5e9c6xL1Cc4ifVo4bFIMuuQemtnb3giV7cKava9luZMDXVTS5p4powtoyb0ACtDGDu9unNkXZkFxC0i7ZSwrZd_aHgim6pFgOWgs0dte0pnb&sa=X&ictx=0&biw=1621&bih=648&dpr=2&ibp=htl;jobs&ved=2ahUKEwjt-4-Y6KyDAxUog4kEHSJ8DjQQudcGKAF6BAgRECo"],
                "maxItems": 50,
                "endPage": 1,
                "queries": [job],
                "languageCode": "en",
                "locationUule": "w+CAIQICIHVG9yb250bw==",
                "radius": 300,
                "includeUnfilteredResults": False,
                "csvFriendlyOutput": True,
                "extendOutputFunction": "($) => { return {} }",
                "customMapFunction": "(object) => { return {...object} }",
                "proxy": { "useApifyProxy": True },
            }
            run = client.actor("nopnOEWIYjLQfBqEO").call(run_input=run_input)   
            data = client.dataset(run["defaultDatasetId"]).list_items().items
            temp = pd.DataFrame(data)
            temp['location'] = locations[location]
            temp['title'] = job
            df = pd.concat([df,temp])
    df.drop(columns=['companyLogo','relatedLinks'], inplace=True)
    df['postedDate'] = df['metadata'].map(extractJobDate)
    df['postedDate'] = df.postedDate.map(dateFormat)
    df['aiSummary'] = df.apply(opneAiSummary,axis=1)

    host = ''
    port = 443
    auth = ('', '') # For testing only. Don't store credentials in code.

    client = OpenSearch(
        hosts = [{'host': host, 'port': port}],
        http_compress = True, # enables gzip compression for request bodies
        http_auth = auth,
        use_ssl = True,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
    )
    index_name = "job_jay"

    if not client.indices.exists(index_name):
        client.indices.create(index=index_name)
    helpers.bulk(client, doc_generator(index_name,df))

    print("Data Saved to ES")
    

    
    return {'message': 'Data Saved to ES'}


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
