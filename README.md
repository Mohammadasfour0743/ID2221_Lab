# ID2221_Lab

These are instructions on how to run the code

- It is recommended to use Linux/WSL

- In linux/WSL terminal, first download python and jdk (if you dont have them): 

    `sudo apt install -y python3 python3-pip python3-venv openjdk-11-jdk`

- Then create a virtual env and activate:

    `python3 -m venv spark-env`

    `source spark-env/bin/activate`

- Install the python packages. Be sure to have the same name since it breaks if versions are different. Also these commands download jupyter notebook:

    `pip install jupyter ipykernel pyspark==3.5.6 delta-spark==3.2.0`

    `python -m ipykernel install --user --name spark-env --display-name "Python (spark-env)"`

- To launch jupyter:

    `jupyter notebook --no-browser`

    Then open the provided link in browser

Everytime you want to work on the project:
- Navigate to repo location
- `source spark-env/bin/activate`
- `jupyter notebook --no-browser`

For the datasets, create a directory in the repo named datasets/ and add the datasets from [here](https://drive.google.com/drive/folders/1qjBtPVDepDE22j0axqrLVR0A2a969Qyy). Make sure to unzip the air_quality dataset and rename it to `air_quality.csv`.

Then you can execute the cells to run the platform.

- To run the queries, simply run the cells. Do not forget to run the ones that load the dataset. The dataset can be obtained by following intructions from week 1
- To generate the analytical data products, run the cells in the section "Making analytical data objects"
- To reproduce the benchmark results, run the cells in order until the "Evaluate platform" where the benchmarks are.