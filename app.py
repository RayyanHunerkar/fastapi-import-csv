import pandas as pd
from fastapi import FastAPI, UploadFile
from pandas import DataFrame
from starlette import status

app = FastAPI()

db = []


async def get_ids(data: DataFrame) -> list:
    ids = []
    for i in range(len(data["ID"])):
        if data.iloc[i]["ID"] != data.iloc[i - 1]["ID"]:
            ids.append(data.loc[i]["ID"])
    return ids


async def get_data(data: DataFrame, ids: list) -> list:
    for j in range(len(ids)):
        soft_skills = []
        summary = []
        technical_skills = []
        for i in range(len(data)):
            if data.loc[i]['ID'] == j + 1:
                if not pd.isna(data.loc[i]['Soft Skills']):
                    soft_skills.append(data.loc[i]['Soft Skills'])
                if not pd.isna(data.loc[i]['Summary']):
                    summary.append(data.loc[i]['Summary'])
                if not pd.isna(data.loc[i]["Technical Skills"]):
                    technical_skills.append(data.loc[i]["Technical Skills"])
                if not pd.isna(data.loc[i]["ID"]):
                    _id = data.loc[i]["ID"]
                if not pd.isna(data.loc[i]["Name"]):
                    name = data.loc[i]["Name"]
                if not pd.isna(data.loc[i]["Email"]):
                    email = data.loc[i]["Email"]
            json = {
                "id": _id,
                "name": name,
                "email": email,
                "soft_skills": soft_skills,
                "technical_skills": technical_skills,
                "summary": summary
            }
        db.append(json)
        return db


@app.post('/', status_code=status.HTTP_201_CREATED)
async def import_csv(file: UploadFile) -> dict:
    data = pd.read_csv(file.file)
    file.file.close()
    data['ID'].fillna(method='ffill', inplace=True)
    ids = await get_ids(data)
    db = await get_data(data, ids)
    return {
        "data": db,
        "message": "data imported succesfully",
        "status": 200
    }
