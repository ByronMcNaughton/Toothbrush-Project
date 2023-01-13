# Toothbrush-Project
---
## Project Architecture  
The project was coded in python, using AWS services to make it publicly accessible.  
There are three main python files;  
**Generate_data.py**  
Generates the sales data for the toothbrushes and handles the upload of this data to the database  
**save_data_to_s3.py**  
generates the json data for the api, then stores this in a file on s3  
**retrieveandsendjsondata.py**  
fetches the api data from s3 and returns it to the user  

### AWS Services:
**Lambda**  
Hosts the python code  
**API Gateway**  
Hosts the endpoint of the Api, links to lambda to return the json data  
**AWS RDS**  
Hosts the database used to store all sales information  
**Step Functions**  
Start/Stop databsae and run lambda functions automatically  

## Explanation  
A few days into the project I noticed that AWS was charging me for RDS Usage, I am a forgetful person so I often left it running when not in use. After a bit of reseach I descovered that step functions could automatically start and stop a RDS instance, this led to me developing a plan to take advantage of the feature. Due to the nature of the generation code, all sales for a day were created in that one run, because of this any api call for that day would always return the same data. I developed the following timeline so that the RDS instance would be runing for the shortest amount of time that necessary.  

1. Start the RDS Instance
2. Wait for it to launch (This is a slow process)
3. Generate orders for that day
4. Generate the api data
5. Save that data as a json to S3
6. Shut down the RDS Instance
7. Return the saved json data instead of running new Queries

I then developed my solution, which was effective in this but limited me in other aspects.  
I wanted to develop a modular API, which took input strings to generate custom queries. I started on this but due to the database being down I had essentially shot myself in the foot, coupled with my limited knowledge of Flask it became a failed(ish) project. 
However I do want to return to this in the future and generate a better solution, I am now aware of flasks intergration with SQLAlchemy, which would make it easy to set up the database and API on an EC2 instance, and create the API as I would have liked to.
Another benefit of moving to an EC2 Instance would be the ability to actually edit my code. Due to limitations with lambda every time I needed to make a change I would need to completly re-upload the project files

Overall I think that the project was successful in expanding my knowledge on AWS, But my biggest take-away is the introduction to Flask and the tools it introduces for web development. 
