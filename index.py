from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
from pymongo import MongoClient, ASCENDING
import pandas as pd
from datetime import datetime
import time
import threading
import requests
import os
from bson import ObjectId

from typing import List
from fastapi.middleware.cors import CORSMiddleware
from models.CourseCreate import CourseCreate
origins = [
    "http://localhost",
    "http://localhost:8000",
    "http://yourdomain.com",
    "https://yourdomain.com",
    # You can also allow all origins by using "*"
]




# MongoDB connection setup (will be handled in the lifespan context)
client = None
db = None
collection = None

# TTL duration in seconds (10 minutes)
TTL_DURATION = 10 * 60

def  load_csv_to_mongo(csv_file: str):
    global collection

    # Download
    url = "https://api.mockaroo.com/api/501b2790?count=100&key=8683a1c0"
    response = requests.get(url)
    if response.status_code == 200:
        # Save the content to a local file
        with open("university.csv", "wb") as file:
            file.write(response.content)
            print("CSV file downloaded and saved as 'downloaded_file.csv'")
    else:
        print(f"Failed to download the file. Status code: {response.status_code}")
    
    # Clear the collection before loading new data
    collection.drop()
    
    # Load the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    
    # Add a field for the TTL index
    df['createdAt'] = datetime.utcnow()

    # Normalize as per spec provided
    df['University'] = df['University'].astype(str)
    df['City'] = df['City'].astype(str)
    df['Country'] = df['Country'].astype(str)
    df['CourseName'] = df['CourseName'].astype(str)
    df['CourseDescription'] = df['CourseDescription'].astype(str)
    df['Currency'] = df['Currency'].astype(str)
    
    # Insert the data into the collection
    collection.insert_many(df.to_dict('records'))
    
    # Ensure TTL index is created
    collection.create_index([("createdAt", ASCENDING)], expireAfterSeconds=TTL_DURATION)

def monitor_collection(csv_file: str):
    while True:
        # Check if the collection is empty
        is_empty = collection.count_documents({}) == 0

        if is_empty:
            if os.path.exists(csv_file):
                    os.remove(csv_file)
                    print(f"{csv_file} has been deleted.")
                    print("Collection is empty, reloading CSV into MongoDB at:", datetime.utcnow().strftime('%a %b %d %Y %H:%M:%S GMT%z (Coordinated Universal Time)'))
                    load_csv_to_mongo(csv_file)
            else:
                print(f"{csv_file} does not exist.")
            
        
        # Sleep for a short period before checking again
        time.sleep(30)  # Check every 30 seconds

async def reload_csv_task():
    while True:
        # Wait for TTL duration
        time.sleep(TTL_DURATION)
        print("Reloading")
        # Reload CSV after the TTL duration
        load_csv_to_mongo("./university.csv")

def convert_id(doc):
    """Convert the ObjectId in MongoDB documents to a string."""
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

@asynccontextmanager
async def lifespan(app: FastAPI):
    global client, db, collection

    # Initialize MongoDB connection
    client = MongoClient("mongodb://mukund:qwertyui@localhost:27017/")
    db = client["test"]
    collection = db["muk"]


    # Load CSV on startup
    load_csv_to_mongo("university.csv")
    print("Loading CSV into MongoDB at:", datetime.utcnow().strftime('%a %b %d %Y %H:%M:%S GMT%z (Coordinated Universal Time)'))


    # Start the monitoring thread
    thread = threading.Thread(target=monitor_collection, args=("university.csv",))
    thread.daemon = True  # Daemonize thread to ensure it exits when the main program does
    thread.start()

    yield  # Application is running

    # Shutdown: Close MongoDB connection
    client.close()

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # List of allowed origins or "*" for all
    allow_credentials=True,  # Allow credentials (cookies, authentication)
    allow_methods=["*"],  # HTTP methods to allow
    allow_headers=["*"],  # HTTP headers to allow
)

@app.get("/")
async def root():
    return {"message": "CSV data is loaded into MongoDB with a TTL of 10 minutes."}

@app.get("/get_all_courses/", response_model=List[dict])
async def getAllCourses():
    documents = list(collection.find())
    documents = [convert_id(doc) for doc in documents]
    return documents

def convert_id(doc):
    """Convert the ObjectId in MongoDB documents to a string."""
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc


@app.delete("/delete_course/{_id}")
async def delete_course(_id: str):
    try:
        # Validate and convert the _id to ObjectId
        if not ObjectId.is_valid(_id):
            raise HTTPException(status_code=400, detail="Invalid ObjectId format")
        
        # Convert the string _id to ObjectId
        object_id = ObjectId(_id)
  
        # Find and delete the document
        result = collection.find_one(ObjectId(_id))
        
        if not result:
            raise HTTPException(status_code=404, detail="Course not found")

        return {"detail": "Course deleted successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/create_course", status_code=status.HTTP_201_CREATED)
def create_course(course: CourseCreate):
    try:
        # Convert the Pydantic model to a dictionary
        course_dict = course.dict()

        # Convert date fields to datetime.datetime
        course_dict['StartDate'] = datetime.combine(course_dict['StartDate'], datetime.min.time())
        course_dict['EndDate'] = datetime.combine(course_dict['EndDate'], datetime.min.time())

        # Add a created_at field for TTL index
        course_dict['created_at'] = datetime.utcnow()

        # Insert the document into the MongoDB collection
        result = collection.insert_one(course_dict)

        # Convert ObjectId to string
        course_dict['_id'] = str(result.inserted_id)
        
        # Return the inserted course data with the MongoDB generated ID
        return convert_id(course_dict)
    except Exception as e:
        # Handle unexpected errors
        raise HTTPException(status_code=500, detail=str(e))




if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
