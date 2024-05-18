 
FROM python:3.9

# 
WORKDIR /code

# 
COPY ./requirements.txt /code/requirements.txt

# Install HDF5 using apt
RUN apt-get update && apt-get install -y libhdf5-dev

# Install h5py with no-binary flag
RUN pip install --no-binary h5py h5py

# Install requirements
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# 
COPY ./app /code/app

# 
CMD ["fastapi", "run", "app/main.py", "--port", "80"]