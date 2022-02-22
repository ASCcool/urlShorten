import os
import asyncio
import shortuuid
import uvicorn
from fastapi import FastAPI, HTTPException, Depends, status, Request 
import pandas as  pd
from decouple import config
from starlette.responses import RedirectResponse
from typing import Optional
from pydantic import BaseModel, validator, constr
import datetime
import validators

try:
    dataBase = pd.read_csv('DataBase.csv').drop(columns=['Unnamed: 0'])
except:
    dataBase = pd.DataFrame(columns=['longUrl','shortUri','shortUrl','creationDatetime','tokenExpireTimeInMins'])

class UrlSchema(BaseModel):
    global dataBase
    longUrl : str 
    shortUri : Optional[constr(max_length = 8)] = None
    tokenExpireTimeInMins : Optional[int] = 5

    @validator('longUrl')
    def validate_url(v):
        if not validators.url(v):
            raise ValueError("Long URL is invalid.")
        return v

app = FastAPI()

@app.get('/')
async def Home():
    return ("Welcome to URL Shortener API. Go to /docs endpoint for testing out the APIs")

@app.post('/shorten')
async def url_shortener(url: UrlSchema):

    global dataBase
    url = dict(url)

    if (url["shortUri"]):
        shortCode = url["shortUri"]
    else:
        shortCode = shortuuid.ShortUUID().random(length = 8)
        url["shortUri"] = shortCode
    
    shortUrl = os.path.join(config("BASE_URL"), shortCode)

    urlExists = dataBase[dataBase["shortUri"]==url["shortUri"]].empty

    if urlExists == False:
        raise HTTPException(status_code = 400, detail = "Short code is invalid, It has been used.")

    try:
        url["shortUrl"] = shortUrl
        url['creationDatetime'] = datetime.datetime.now()
        dataBase = dataBase.append(url,ignore_index=True)
        dataBase.to_csv('DataBase.csv')

        return {
            "message" : "Successfully shortened URL.",
            "shortUrl" : shortUrl,
            "longUrl" : url["longUrl"]
        }
        
    except Exception as e:
        print(e)
        raise HTTPException(status_code = 500, detail = "An unknown error occurred.")
    
@app.delete('/{shortUri}')
async def delete_token(shortUri: str):
    global dataBase
    idx = list(dataBase.index[dataBase["shortUri"]==shortUri])[0]
    print("Syncing with database...")
    await asyncio.sleep(dataBase.loc[idx]['tokenExpireTimeInMins']*60)
    dataBase.drop(idx,inplace=True)
    dataBase.to_csv('DataBase.csv')
    print("Token Expired for Uri",shortUri)
    return True    

@app.get('/{shortUri}')
async def redirect_url(shortUri: str):

    global dataBase

    url = dataBase[dataBase["shortUri"]==shortUri]
    idx = list(dataBase.index[dataBase["shortUri"]==shortUri])[0]
    url = url.to_dict(orient="index")[idx]

    if not url:
        raise HTTPException(status_code= 404, detail = "URL not found !")

    else:
        print("Printing shorturl",url["shortUrl"])
        response = RedirectResponse(url = url["longUrl"])
        return response

if __name__ == "__main__":
    uvicorn.run(app, host  = "0.0.0.0", port = 8000)