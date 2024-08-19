from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status, Body, Query
from pymongo import MongoClient, ASCENDING
import pandas as pd
from datetime import datetime
import time
import threading
import requests
import os
from bson import ObjectId
import logging
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

# Pagination api new
@app.post("/get_all_courses_new/", response_model=List[dict])
async def get_all_courses_new(page: int = Query(1, ge=1), limit: int = Query(10, ge=1, le=100)):
    try:
        # Calculate the number of documents to skip
        skip = (page - 1) * limit

        # Fetch documents from MongoDB with pagination
        cursor = collection.find().skip(skip).limit(limit)
        documents = list(cursor)

        # Convert ObjectId to string for JSON serialization
        documents = [convert_id(doc) for doc in documents]

        return documents

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def convert_id(doc):
    """Convert the ObjectId in MongoDB documents to a string."""
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc

@app.post("/delete-course")
async def delete_course(payload: dict = Body(...)):
    # Extract id from the payload
    id = payload.get("id")
    
    if not id:
        raise HTTPException(status_code=400, detail="ID is required")

    # Step 1: Validate the ObjectId
    try:
        object_id = ObjectId(id)
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid course ID format")

    # Step 2: Attempt to find and delete the course in the MongoDB collection
    result = collection.find_one_and_delete({"_id": object_id})

    if result is None:
        raise HTTPException(status_code=404, detail="Course not found")

    # Step 3: Return a success message
    return {"message": "Course deleted successfully!", "course_id": id}

# @app.patch("/update-course", )
# def update_course(course: CourseCreate):
#     # Validate the _id field
#     if not course._id:
#         raise HTTPException(status_code=400, detail="Course ID (_id) is required for updating.")
#     object_id = None
#     # Convert the _id to ObjectId
#     try:
#         object_id = ObjectId(course._id)
#         print("Objec", object_id)
#     except Exception as e:
#         raise HTTPException(status_code=400, detail="Invalid course ID format.")

#     # Find the course by _id
#     existing_course = collection.find_one({"_id": object_id})
#     if not existing_course:
#         raise HTTPException(status_code=404, detail="Course not found." + object_id)

#     # Prepare the update data
#     update_data = {k: v for k, v in course.dict().items() if v is not None and k != "_id"}

#     # Update the course in the database
#     collection.update_one({"_id": object_id}, {"$set": update_data})

from datetime import datetime

def to_mongo_dict(course_data):
    return {
        "University": course_data.University,
        "City": course_data.City,
        "Country": course_data.Country,
        "CourseName": course_data.CourseName,
        "CourseDescription": course_data.CourseDescription,
        "StartDate": datetime.strptime(course_data.StartDate, "%Y-%m-%d"),
        "EndDate": datetime.strptime(course_data.EndDate, "%Y-%m-%d"),
        "Price": course_data.Price,
        "Currency": course_data.Currency,
        "createdAt": datetime.strptime(course_data.createdAt, "%Y-%m-%dT%H:%M:%S.%fZ") if course_data.createdAt else None,
    }


#     return {"message": "Course updated successfully", "course_id": course._id, "object" : object_id}
logger = logging.getLogger('uvicorn.error')
logger.setLevel(logging.DEBUG)
@app.post("/update-course")
async def update_course(course: dict = Body(...)):
    # Manually validate the _id field
    if '_id' not in course:
        raise HTTPException(status_code=400, detail="Course ID (_id) is required for updating.")
    try:
        object_id = ObjectId(course['_id'])
        logger.debug("ll", object_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail="Invalid course ID format.")

    # Find the course by _id
    existing_course = collection.find_one(ObjectId(object_id))
    if not existing_course:
        raise HTTPException(status_code=404, detail=f"Course not found for ID: {course['_id']}")

    # Prepare the update data, removing the _id key
    update_data = {k: v for k, v in course.items() if k != "_id"}

    # Update the course in the database
    result = collection.update_one({"_id": object_id}, {"$set": update_data})

    if result.modified_count == 0:
        raise HTTPException(status_code=304, detail="No modifications made to the course.")

    return {"message": "Course updated successfully", "course_id": str(object_id)}
from typing import Optional

@app.put("/submit-course")
async def submit_course(
    id: str = Body(...),
    University: str = Body(...),
    City: str = Body(...),
    Country: str = Body(...),
    CourseName: str = Body(...),
    CourseDescription: str = Body(...),
    StartDate: str = Body(...),
    EndDate: str = Body(...),
    Price: float = Body(...),
    Currency: str = Body(...),
    createdAt: Optional[str] = Body(None)
):
    # Step 1: Find the course in the MongoDB collection using `id`
    existing_course = collection.find_one({"_id": ObjectId(id)})
    if not existing_course:
        raise HTTPException(status_code=404, detail="Course not found")
    
    # Step 2: Create a CourseCreate Pydantic model instance with the new data
    course_data = CourseCreate(
        University=University,
        City=City,
        Country=Country,
        CourseName=CourseName,
        CourseDescription=CourseDescription,
        StartDate=StartDate,
        EndDate=EndDate,
        Price=Price,
        Currency=Currency,
        createdAt=createdAt
    )
    
    # Step 3: Update the existing course in the MongoDB collection
    update_result = collection.update_one(
        {"_id": ObjectId(id)},
        {"$set":to_mongo_dict( course_data)}
    )
    
    if update_result.modified_count == 0:
        raise HTTPException(status_code=400, detail="Course update failed")
    
    # Step 4: Retrieve the updated course to return in the response
    updated_course = collection.find_one({"_id": ObjectId(id)})

    # Return a success message with the updated data
    return {"message": "Course updated successfully!", "data": updated_course}

def to_mongo_dict(self):
        return {
            "University": self.University,
            "City": self.City,
            "Country": self.Country,
            "CourseName": self.CourseName,
            "CourseDescription": self.CourseDescription,
            "StartDate": datetime.strptime(self.StartDate, "%Y-%m-%d"),
            "EndDate": datetime.strptime(self.EndDate, "%Y-%m-%d"),
            "Price": self.Price,
            "Currency": self.Currency,
            "createdAt": datetime.strptime(self.createdAt, "%Y-%m-%dT%H:%M:%S.%fZ"),
        }

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
